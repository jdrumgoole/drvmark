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
import "reflect"
import "encoding/json"

// Author : Joe.Drumgoole@mongodb.com

var wg sync.WaitGroup

const arrObjSize = 10
const language = "go" 

	
func check(e error) {
	if e != nil {
		fmt.Printf( "error: %s", e )
		panic(e)
	}
}

// Can't extend other packages directly 
type logger mgo.Collection

func make_log_collection( session *mgo.Session ) logger {
	db := session.DB( "results" )
	return logger( *db.C( "driver" ))
}

func ( c* logger ) to_col() *mgo.Collection {
	col := mgo.Collection( *c )
	return &col
}

func ( c *logger ) drop() {
	c.to_col().DropCollection()
}

func ( c *logger ) log_elapsed( t time.Duration, key string ) {
	c.to_col().Upsert( bson.M{ "_id" : language }, bson.M{ "$set": bson.M{ key  : int64( t ) }} )
}

func ( c *logger ) log_max( t time.Duration, key string ) {
	c.to_col().Upsert( bson.M{ "_id" : language }, bson.M{ "$max" : bson.M{ key  : int64( t ) }} )
}

func (  c *logger ) delete_entry() {
	c.to_col().Remove( bson.M{ "_id" : language } ) 
}


func pretty( d bson.M ) string{
	b, err := json.MarshalIndent(d, "", "  ")
	check( err )
	return string( b )
	
}

func pretty_print( d bson.M ) {
	fmt.Println( pretty( d ))
}

func generate_record( threadNum int, recnum int ) bson.M {

	// Definition of a 'Document'
	topFields := 20 //20 top level fields
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
	
	//fmt.Printf( "bulk_inserter called: %d\n", threadNum )
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

func create_records( collection *mgo.Collection, numRecords int, threadnum int, batch bool ) time.Duration {
	//fmt.Printf( "t.section: '%s'", t.section ) 
	// Drop the test collection
	
	start := time.Now()
	//fmt.Printf( "Dropping : %s\n", collection.Name ) 
	collection.DropCollection()

	if batch {
		//fmt.Print( "Batch inserter\n" )
		bulk_inserter( collection, threadnum, numRecords, 1000 )
	} else {
		//fmt.Print( "Single Inserter\n" )
		for i :=1 ; i <= numRecords ; i++ {
			single_inserter( collection, threadnum, i )
		}
	}
	return time.Since( start )	
}

func update_records( collection *mgo.Collection, numRecords int, threadnum int, batch bool ) time.Duration {

	start := time.Now()
	var result interface{}
	
	iterator := collection.Find( bson.M{}).Iter()
	
	for iterator.Next( &result ) {
		resultDoc := result.( bson.M )
		for k,v := range resultDoc {

			if ( k != "_id" )  && (  reflect.TypeOf( v ).Kind() == reflect.String ) { 
					modified := v.(string) + " (modified)"
					resultDoc[ k ] = modified
			}
			collection.Update( bson.M{ "_id" : resultDoc[ "_id" ] }, resultDoc )
		}
	}
	return time.Since( start )
}

func read_records( collection *mgo.Collection, numRecords int, threadnum int, batch bool ) time.Duration {
	
	start := time.Now()
	iterator := collection.Find( bson.M{}).Iter()
	var result interface{}
	var sum float64

	for iterator.Next( &result) {
		doc := result.( bson.M  )
		subArray := doc[ "arr" ].( []interface{} )
		for _, v := range subArray {
			doc := v.( bson.M )
			sum = sum + doc["subval2"].(float64)
		}
	}
	
	return time.Since( start )
}

func single_thread_test( session *mgo.Session, section string, numRecords int, threadnum int, batch bool ) time.Duration {
	
	defer wg.Done()
	//defer fmt.Printf( "Finishing thread : %d\n", threadnum ) 
	
	var elapsed time.Duration
	
	var totalElapsed time.Duration
	
	var key string
	
	//fmt.Printf( "Starting thread : %d\n", threadnum ) 
	
	if threadnum == 0 {
		key = "linear." + section
	} else {
		key = "parallel." + section
	}
	
	results := session.Copy()
	defer results.Close()
	db := results.DB( "drvmark-go" )
	collection_name := "records_" + strconv.Itoa( threadnum )
	collection := db.C( collection_name )
	log_collection := make_log_collection( results )
	if section == "create" || section == "all" {
		elapsed = create_records( collection, numRecords, threadnum, batch )
		log_collection.log_max( elapsed, key )
		totalElapsed = totalElapsed + elapsed
	}
	
	if section == "read" || section == "all"  {
		elapsed = read_records( collection, numRecords, threadnum, batch )
		log_collection.log_max( elapsed, key )
		totalElapsed = totalElapsed + elapsed 
	}
	
	if section == "update" || section == "all" {
		elapsed = update_records( collection, numRecords, threadnum, batch )
		log_collection.log_max( elapsed, key )
		totalElapsed = totalElapsed + elapsed
	}
	return totalElapsed
}

func multi_threaded_test( session *mgo.Session, section string, numRecords int, threadnum int, batch bool ) time.Duration {
	
	
	start :=time.Now()
	remainder := numRecords % threadnum
	chunk := numRecords / threadnum
	for i:=1 ; i <=  threadnum ; i++ {
		wg.Add( 1 )
		if ( i == threadnum ) {
			chunk = chunk + remainder
		}
		go single_thread_test( session, section, chunk, i, batch ) 
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

	fmt.Printf( "Batch is: %t\n", batch )
	fmt.Printf( "Record count is: %d\n", numRecords ) 
	

	data, err := ioutil.ReadFile("./connection_string")
	check(err)
	connection_string := strings.TrimSpace( string( data ))
	fmt.Printf( "Connection string: '%s'\n", connection_string )
	session, err  := mgo.Dial( connection_string )
	check(err)
	defer session.Close()
	
	sections := []string{ "create", "update", "read" } 
	session.SetMode(mgo.Monotonic, true)
	threadnum := 4
	log_collection := make_log_collection( session )
	log_collection.delete_entry()
	
	for _, section := range sections {
		fmt.Printf( "Starting section : %s\n", section )
		wg.Add( 1 )
		st_time := single_thread_test( session, section, numRecords, 0,  batch ) 
		wg.Wait()
		log_collection.log_elapsed( st_time, "linear.total" ) 
		
		mt_time := multi_threaded_test( session, section, numRecords, threadnum, batch )
		
		log_collection.log_elapsed( mt_time, "parallel.total" )
		
		fmt.Printf( "ST Elapsed time: %s\n", st_time )
		fmt.Printf( "MT Elapsed time: %s\n", mt_time )
	
		total_duration := mt_time + st_time
		
		
		fmt.Printf( "Total elapsed time: %s seconds\n", total_duration )
		fmt.Printf( "Ending section : %s\n", section )
	}

}
