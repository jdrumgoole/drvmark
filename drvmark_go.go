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

// Author : Joe.Drumgoole@mongodb.com

func check(e error) {
	if e != nil {
		fmt.Printf( "error: %s", e )
		panic(e)
	}
}

func process_batch( bulk *mgo.Bulk, collection *mgo.Collection ) {
/*
	fatal_error := false
	success := false
	retries := 0
	max_retries := 10
*/
	
	// if len( 
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
	id := string((recnum % 256)) + "-" + string(recnum)

	rec := bson.M{ "_id" : id }
	
	for  i :=0; i <= topFields ; i++ {
		tp := i % 4
		if tp == 0 {
			strValue := "Lorem ipsum dolor sit amet, consectetur adipiscing elit." // text
			rec[fldpfx + string(i)] = strValue
		} else if tp == 1 {
			dateValue:= time.Unix(int64((i * recnum)/1000), 0 ) //A date
			rec[fldpfx + string(i)] = dateValue
		} else if  tp == 2 {
			floatValue := math.Pi * float64( i ) //A float
			rec[fldpfx + string(i)] = floatValue
		} else {
			intValue := int64(recnum + i) // A 64 bit integer
			rec[fldpfx + string(i)] = intValue
		}
	}


	var arrayObject []interface{} = make( []interface{}, arrObjSize )
	
	for  i :=0; i < arrSize ; i++ {
		subRec := bson.M{ "name" : "subRec" }
		for j :=0; j < arrObjSize; j++ {
			tp := j % 4
			if tp == 0 {
				strValue := "Nunc finibus pretium dignissim. Aenean ut nisi finibus." // text
				subRec["subval" + string(j)] = strValue
			} else if tp == 1 {
				dateValue:= time.Unix(int64((i * recnum)/1000), 0 ) //A date
				subRec["subval" + string(j)] = dateValue
			} else if  tp == 2 {
				floatValue := math.Pi * float64( i ) //A float
				subRec["subval" + string(j)] = floatValue
			} else {
				intValue := int64(recnum + i) // A 64 bit integer
				subRec["subval" + string(j)] = intValue
			}
			arrayObject[ j ] = subRec
		}
		rec[ "arr" ] = arrayObject
	}
	
	//return rec
	return bson.M{ "a" : "b" }
}

func single_thread_test( t *TestData ) {

	//batchSize := 1000
	
	fmt.Printf( "num threads: %d\n", t.threadnum ) 

	session, err := mgo.Dial( t.connectionString )
	check( err )
	db := session.DB( "drvmark-" + t.language )

	test_collection := db.C( "records_" + string( t.threadnum ))

	if ( t.section == "create")  || ( t.section == "all" ) {
		   // Drop the test collection
		   test_collection.DropCollection()   
	}

	//bulker := test_collection.Bulk()
	var rec bson.M
	for i :=1 ; i <= t.numRecords ; i++ {
		rec = generate_record( t.threadnum, i+1 )
		fmt.Printf( "type: %T value: %v\n", rec, rec )
		error := test_collection.Insert( rec )
		check( error ) 
		//fmt.Printf( "Inserting: %s", rec )
		//bulker.Insert( rec )
//		fmt.Printf( "i is %d\n", i )
//		if ( i % batchSize ) == 0 {
//			_, err := bulker.Run()
//			check( err )
//			bulker = test_collection.Bulk()
//			fmt.Printf( "%d inserted", i )
//		}
	}
}


func main() {

	var numRecords int
	var argErr error
	
	if len( os.Args ) > 1 {
		numRecords, argErr = strconv.Atoi( os.Args[ 1 ] )
		check( argErr )

	} else {
		numRecords = 1000000 // one million
	}

	language := "go"
	verbose := true
/*

	
	topFields := 20
	arrObjSize := 10
	arrSize := 20
        */

	data, err := ioutil.ReadFile("./connection_string")
	check(err)
	connection_string := strings.TrimSpace( string( data ))
	fmt.Printf( "Connection string: '%s'\n", connection_string )

	testdata := TestData{
		threadnum : 0,
		connectionString : connection_string,
		numRecords : numRecords,
		language : language,
		mode  : "linear",
		verbose : verbose,
		section : "all",
	}


	session, err  := mgo.Dial( connection_string )
	check(err)
	defer session.Close()

	session.SetMode(mgo.Monotonic, true)
	
	resultsDB := session.DB("results")
	log_collection := resultsDB.C( "driver" )
	log_collection.Remove(bson.M{"_id":language})

	start := time.Now()
	
	single_thread_test( &testdata ) 
	
	elapsed := time.Since( start )
	_, err = log_collection.Upsert( bson.M{"_id":language}, bson.M{ "$set" : bson.M{ "linear.time" : int64( time.Since( start )) }} )
	check( err ) 
	fmt.Printf( "Elapsed time: %s seconds\n", elapsed )

}
