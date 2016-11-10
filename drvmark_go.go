package main

import "fmt"
import "io/ioutil"
import "gopkg.in/mgo.v2"
import "gopkg.in/mgo.v2/bson"
import "strings"
import "time"
import "math"
import "os"
import "strconv"
import "sync"

// Author : Joe.Drumgoole@mongodb.com

var wg sync.WaitGroup
	
func check(e error) {
	if e != nil {
		fmt.Printf( "error: %s", e )
		panic(e)
	}
}

type TestData struct {
	
	threadnum int
	connectionString string
	numRecords int
	language string
	mode string
	verbose bool
	section string
}

type ElapsedTime struct {
	linear time.Duration
	parallel time.Duration
}

type FileRecord struct {
	ID        string "_id"
	arr       []interface{}
}

func generate_record( threadNum int, recnum int ) bson.M {

	// Definition of a 'Document'
	topFields := 20 //20 top level fields
	arrObjSize := 10 // 10 fields in each array object
	arrSize := 20 // Array of 20 elements
	//verbose := true
	fldpfx  := "val"
	id := strconv.Itoa(recnum % 256) + "-" + strconv.Itoa(recnum)
	
	rec := bson.M{ "_id" : id }
	
	for  i :=0; i <= topFields ; i++ {
		tp := i % 4
		if tp == 0 {
			strValue := "Lorem ipsum dolor sit amet, consectetur adipiscing elit." // text
			rec[fldpfx + strconv.Itoa(i)] = strValue
		} else if tp == 1 {
			dateValue:= time.Unix(int64((i * recnum)/1000), 0 ) //A date
			rec[fldpfx + strconv.Itoa(i)] = dateValue
		} else if  tp == 2 {
			floatValue := math.Pi * float64( i ) //A float
			rec[fldpfx + strconv.Itoa(i)] = floatValue
		} else {
			intValue := int64(recnum + i) // A 64 bit integer
			rec[fldpfx + strconv.Itoa(i)] = intValue
		}
	}


	var arrayObject []interface{} = make( []interface{}, arrObjSize )
	
	for  i :=0; i < arrSize ; i++ {
		subRec := bson.M{ "name" : "subRec" }
		for j :=0; j < arrObjSize; j++ {
			tp := j % 4
			if tp == 0 {
				strValue := "Nunc finibus pretium dignissim. Aenean ut nisi finibus." // text
				subRec["subval" + strconv.Itoa(j)] = strValue
			} else if tp == 1 {
				dateValue:= time.Unix(int64((i * recnum)/1000), 0 ) //A date
				subRec["subval" + strconv.Itoa(j)] = dateValue
			} else if  tp == 2 {
				floatValue := math.Pi * float64( i ) //A float
				subRec["subval" + strconv.Itoa(j)] = floatValue
			} else {
				intValue := int64(recnum + i) // A 64 bit integer
				subRec["subval" + strconv.Itoa(j)] = intValue
			}
			arrayObject[ j ] = subRec
		}
		rec[ "arr" ] = arrayObject
	}
	
	return rec

}

func bulk_inserter( collection *mgo.Collection, threadNum int, numRecords int, batchSize int ) {
	bulker := collection.Bulk()
	bulker.Unordered()
	var rec bson.M
	var total int
	
	fmt.Printf( "bulk_inserter called: %d\n", threadNum )
	for i :=1 ; i <= numRecords ; i++ {
		rec = generate_record( threadNum, i )
		//fmt.Printf( "%d added %s\n", i, rec[ "_id" ] )
		bulker.Insert( rec )
		if ( i % batchSize ) == 0 {
			_, err := bulker.Run()
			check( err )
			fmt.Printf( "Batch inserted: %d\n", i )
			bulker = collection.Bulk()
		}
		total = i 
	}

	_, err := bulker.Run()
	check( err )
	fmt.Printf( "Total Batch inserted : %d\n", total )
}

func single_inserter( collection *mgo.Collection, threadNum int, count int ) {
	var rec bson.M
	rec = generate_record( threadNum, count )
	error := collection.Insert( &rec )
	//fmt.Printf( "%d inserted: %v\n", count, rec )
	check( error )
}

func single_thread_test( session *mgo.Session, section string, numRecords int, threadnum int, batch bool ) time.Duration {
	
	defer wg.Done()
	defer fmt.Printf( "Finishing thread : %d\n", threadnum ) 
	
	fmt.Printf( "Starting thread : %d\n", threadnum ) 
	
	results := session.Copy()
	defer results.Close()
	db := results.DB( "drvmark-go" )
	collection_name := "records_" + strconv.Itoa( threadnum )
	
	test_collection := db.C( collection_name )


	//fmt.Printf( "t.section: '%s'", t.section ) 
	if  ( section == "create")  || ( section =="all" ) {
		   // Drop the test collection
		   fmt.Printf( "Dropping : %s:%s\n", db.Name, collection_name ) 
		   test_collection.DropCollection()
		   //check( error )  
	}
	
	fmt.Printf( "Writing to %s:%s\n", db.Name, collection_name ) 

	start := time.Now()
	if batch {
		fmt.Print( "Batch inserter\n" )
		bulk_inserter( test_collection, threadnum, numRecords, 1000 )
	} else {
		fmt.Print( "Single Inserter\n" )
		for i :=1 ; i <= numRecords ; i++ {
			single_inserter( test_collection, threadnum, i )
		}
	}
	return time.Since( start )
}

func multi_threaded_test( session *mgo.Session, section string, numRecords int, threadnum int, batch bool ) time.Duration {
	
	
	start :=time.Now()
	for i:=1 ; i <=  threadnum ; i++ {
		wg.Add( 1 )
		go single_thread_test( session, section, numRecords, i, batch ) 
	}
	wg.Wait()
	
	return time.Since( start )
}

func main() {

	var numRecords int
	var argErr error
	var batch bool = true
	
	if len( os.Args ) > 1 {
		numRecords, argErr = strconv.Atoi( os.Args[ 1 ] )
		check( argErr )

	} else {
		numRecords = 1000000 // one million
	}
	
	if len( os.Args ) > 2 {
		batchval := os.Args[ 2 ] 
		check( argErr )
		if ( batchval == "false" ) {
			batch = false
		}
	}

	language := "go"
	//verbose := true


	fmt.Printf( "Batch is: %t\n", batch )
	fmt.Printf( "Record count is: %d\n", numRecords ) 
	
//	testdata := TestData{
//		threadnum : 0,
//		numRecords : numRecords,
//		language : language,
//		mode  : "linear",
//		verbose : verbose,
//		section : "all",
//	}

	data, err := ioutil.ReadFile("./connection_string")
	check(err)
	connection_string := strings.TrimSpace( string( data ))
	fmt.Printf( "Connection string: '%s'\n", connection_string )
	session, err  := mgo.Dial( connection_string )
	check(err)
	defer session.Close()


	
	session.SetMode(mgo.Monotonic, true)
	threadnum := 4
	resultsDB := session.DB("results") 
	log_collection := resultsDB.C( "driver" )
	log_collection.Remove(bson.M{"_id":language})
	
	wg.Add( 1 )
	st_time := single_thread_test( session, "all", numRecords, 0,  batch ) 
	wg.Wait()

	
	mt_time := multi_threaded_test( session, "all", numRecords/threadnum, threadnum, batch )
	
	fmt.Printf( "ST Elapsed time: %s\n", st_time )
	fmt.Printf( "MT Elapsed time: %s\n", mt_time )

	total_duration := mt_time //+ st_time
	
	_, err = log_collection.Upsert( bson.M{"_id":language}, bson.M{ "$set" : bson.M{ "linear.time" : int64( total_duration )}} )
	check( err ) 
	fmt.Printf( "Toal elapsed time: %s seconds\n", total_duration )

}
