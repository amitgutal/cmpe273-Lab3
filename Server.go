package main
import  (
		"fmt"
		"net/http"
		"sort"
		"strconv"
		"strings"
		"encoding/json"
		"github.com/julienschmidt/httprouter"
		)


type CountElements struct{
	Node string   `json:"node"`
	Count int	  `json:"count"`
}

type KeyValue struct{
	
	Key int	`json:"key,omitempty"`
	Value string	`json:"value,omitempty"`
}

var key1,key2,key3 [] KeyValue

var location1,location2,location3 int

type Mapping struct{
	Node string `json:"node"`	
	Key int `json:"key"`
}

type ForMapping []Mapping

func (a ForMapping) Len() int           {
   return len(a) 
}
func (a ForMapping) Swap(i, j int)      { 

    a[i], a[j] = a[j], a[i] 
}
func (a ForMapping) Less(i, j int) bool {
    return a[i].Key < a[j].Key
 }

type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


func AllRedisKeyValues(respWriter http.ResponseWriter, request *http.Request,p httprouter.Params){
	
	var output []KeyValue
	
	for _,i:=range key1{
		
		output = append(output,i)
	}
	for _,i:=range key2{
		
		output = append(output,i)
	}
	for _,i:=range key3{
		
		output = append(output,i)
	}
	sort.Sort(ByKey(output))
	result,_:= json.Marshal(output)
	fmt.Fprintln(respWriter,string(result))
}

func PingRedisPorts(respWriter http.ResponseWriter, request *http.Request,redRouter httprouter.Params){
	
	redisport := strings.Split(request.Host,":")
	
	if(redisport[1]=="3000"){
		
		sort.Sort(ByKey(key1))
		result,_:= json.Marshal(key1)
		fmt.Fprintln(respWriter,string(result))
		
	}else if(redisport[1]=="3001"){
		
		sort.Sort(ByKey(key2))
		result,_:= json.Marshal(key2)
		fmt.Fprintln(respWriter,string(result))
	}else{
		
		sort.Sort(ByKey(key3))
		result,_:= json.Marshal(key3)
		fmt.Fprintln(respWriter,string(result))
	}
}

func AddKeyValues(respWriter http.ResponseWriter, request *http.Request,redRoute httprouter.Params){
	
	port := strings.Split(request.Host,":")
	key,_ := strconv.Atoi(redRoute.ByName("key_id"))
	
	if(port[1]=="3000"){
		key1 = append(key1,KeyValue{key,redRoute.ByName("value")})
		location1++
	}else if(port[1]=="3001"){
		key2 = append(key2,KeyValue{key,redRoute.ByName("value")})
		location2++
	}else{
		key3 = append(key3,KeyValue{key,redRoute.ByName("value")})
		location3++
	}	
}

func GetMapping(respWriter http.ResponseWriter, request *http.Request,redisRoute httprouter.Params){
	
	var maps[] Mapping
	var j,k,l,point1,point2,point3 int
	j=0
	k=0
	l=0 
	point1=0
	point2=0
	point3=0
	
	for i:=0;i<(location1+location2+location3); i++{
		if(key1!=nil && point1!=1){
			maps = append(maps,Mapping{"http://localhost:3000",key1[j].Key})
			if(j < len(key1)-1){
				j++
			}else{
				point1=1
			}
		}
		if(key2!=nil && point2!=1){
			maps = append(maps,Mapping{"http://localhost:3001",key2[k].Key})
			if(k < len(key2)-1){
				k++
			}else{
				point2 =1
			}
		}
		if(key3!=nil && point3!=1){
			maps = append(maps,Mapping{"http://localhost:3002",key3[l].Key})
			if(l < len(key3)-1){
				l++
		    }else{
		    	point3=1
		    }
		}
	}
	sort.Sort(ForMapping(maps))
	result,_:= json.Marshal(maps)
	fmt.Fprintln(respWriter,string(result))
}

func GetOneKey(respWriter http.ResponseWriter, request *http.Request,routerp httprouter.Params){	
	
	indexKey := location1
	output := key1

	
	port := strings.Split(request.Host,":")
	
	if(port[1]=="3001"){
		
		output = key2 
		indexKey = location2
		
	}else if(port[1]=="3002"){
		
		output = key3
		indexKey = location3
	}	
	
	key,_ := strconv.Atoi(routerp.ByName("key_id"))
	
	for i:=0 ; i< indexKey ;i++{
		
		if(output[i].Key==key){
			
			result,_:= json.Marshal(output[i])
			fmt.Fprintln(respWriter,string(result))
			
		}
	}
}




func main(){
	
	location2 = 0
	location3 = 0
	location1 = 0
	
	mutliplexer := httprouter.New()
	
    mutliplexer.GET("/keys",PingRedisPorts)
	
	mutliplexer.GET("/keys/:key_id",GetOneKey)
	
	mutliplexer.PUT("/keys/:key_id/:value",AddKeyValues)
 
	mutliplexer.GET("/all",AllRedisKeyValues)
	
    mutliplexer.GET("/mapping",GetMapping)
	
    go http.ListenAndServe(":3000",mutliplexer)
	
    go http.ListenAndServe(":3001",mutliplexer)
	
    go http.ListenAndServe(":3002",mutliplexer)
	
    select {}
}