package main  

import (  
    "fmt"  
    "hash/crc32"  
    "sort"     
    "net/http"
    "encoding/json" 
    "io/ioutil"
	"strconv")
   
type HashCircle []uint32  

type KeyValue struct{
    Key int `json:"key,omitempty"`
    Value string `json:"value,omitempty"`
}

type Mapping struct{
    NodeAddress string `json:"node,omitempty"`   
    Key int `json:"key,omitempty"`
}

type CountElements struct{
    NodeIP string   `json:"node,omitempty"`
    Count int     `json:"count,omitempty"`
}

func (hr HashCircle) Len() int {  
    return len(hr)  
}  
  
func (hr HashCircle) Less(i, j int) bool {  
    return hr[i] < hr[j]  
}  
  
func (hr HashCircle) Swap(i, j int) {  
    hr[i], hr[j] = hr[j], hr[i]  
}  
 
func NewNode(id int, ip string) *Node {  
    return &Node{  
        Id:       id,  
        IP:       ip,  
    }  
}  

type Node struct {  
    Id       int  
    IP       string    
}  
  

  

  
func ConsistentHashingAlgo() *ConsistentHash {  
    return &ConsistentHash{  
        Nodes:     make(map[uint32]Node),   
        IsPresent: make(map[int]bool),  
        Circle:      HashCircle{},  
    }  
}  
type ConsistentHash struct {  
    Nodes       map[uint32]Node  
    IsPresent   map[int]bool  
    Circle      HashCircle  
    
}    
func (hr *ConsistentHash) AddNode(node *Node) bool {  
 
    if _, ok := hr.IsPresent[node.Id]; ok {  
        return false  
    }  
    str := hr.GetIPAddress(node)  
    hr.Nodes[hr.HashValue(str)] = *(node)
    hr.IsPresent[node.Id] = true  
    hr.SortHashCircle()  
    return true  
}  
  
func (hr *ConsistentHash) SortHashCircle() {  
    hr.Circle = HashCircle{}  
    for k := range hr.Nodes {  
        hr.Circle = append(hr.Circle, k)  
    }  
    sort.Sort(hr.Circle)  
}  
  
func (hr *ConsistentHash) GetIPAddress(node *Node) string {  
    return node.IP 
}  
  
func (hr *ConsistentHash) HashValue(key string) uint32 {  
    return crc32.ChecksumIEEE([]byte(key))  
}  
  
func (hr *ConsistentHash) Get(key string) Node {  
    hash := hr.HashValue(key)  
    i := hr.TrackingNodes(hash)  
    return hr.Nodes[hr.Circle[i]]  
}  

func (hr *ConsistentHash) TrackingNodes(hash uint32) int {  
    i := sort.Search(len(hr.Circle), func(i int) bool {return hr.Circle[i] >= hash })  
    if i < len(hr.Circle) {  
        if i == len(hr.Circle)-1 {  
            return 0  
        } else {  
            return i  
        }  
    } else {  
        return len(hr.Circle) - 1  
    }  
}  
 
func GetKeyValue(key string,circle *ConsistentHash){
    var out KeyValue 
    ipAddress:= circle.Get(key)
    response,keyvalue_error:= http.Get("http://"+ipAddress.IP+"/keys/"+key)
    if keyvalue_error!=nil{
        fmt.Println("Error in Getting Key Values from the Servers ",keyvalue_error)
    }else{
        defer response.Body.Close()
        contents,err:= ioutil.ReadAll(response.Body)
        if(err!=nil){
			fmt.Println(" Error in Key Value Pairs ")
            fmt.Println(err)
        }
        json.Unmarshal(contents,&out)
        result,_:= json.Marshal(out)
        fmt.Println(string(result))
    }
}

func PutKeyValues(circle *ConsistentHash){
        var str,input string
        fmt.Print("Enter the Key :")
        fmt.Scanf("%s\n",&str)
        fmt.Print("Enter the Value:")
        fmt.Scanf("%s\n",&input)
        ipAddress := circle.Get(str)  
        address := "http://"+ipAddress.IP+"/keys/"+str+"/"+input
        req,err := http.NewRequest("PUT",address,nil)
        client := &http.Client{}
        resp, err := client.Do(req)
        if err!=nil{
            fmt.Println("Error:",err)
        }else{
            defer resp.Body.Close()
            fmt.Println(resp.StatusCode)
        }  
}  



func GetKeyValues(circle *ConsistentHash){
    var out []KeyValue
    response,err:= http.Get("http://127.0.0.1:3000/all")
    if err!=nil{
        fmt.Println("Error:",err)
    }else{
        defer response.Body.Close()
        contents,err:= ioutil.ReadAll(response.Body)
        if(err!=nil){
            fmt.Println(err)
        }
        json.Unmarshal(contents,&out)
        result,_:= json.Marshal(out)
        fmt.Println(string(result))
    }
}

func MappedOutput(){
	
    var output[] Mapping
    response,err_maps:= http.Get("http://127.0.0.1:3000/mapping")
    if err_maps!=nil{
        fmt.Println("Error in processing Mapped Output:",err_maps)
    }else{
        
		defer response.Body.Close()
        
		contents,err_Output:= ioutil.ReadAll(response.Body)
        
		if(err_Output!=nil){
            fmt.Println(err_Output)
        }
        
		json.Unmarshal(contents,&output)
        
		for _,j := range output{
			
            fmt.Println("\n Key Value "+ strconv.Itoa(j.Key)+" ----->> "+j.NodeAddress)
        } 
    }   
}

func GetSpecificPort(){
     
    var port string
    fmt.Println("Enter the port no:")
    fmt.Scanf("%s\n",&port)
    var out []KeyValue
    response,err:= http.Get("http://127.0.0.1:"+port+"/keys")
    if err!=nil{
        fmt.Println("Error:",err)
    }else{
        defer response.Body.Close()
        contents,err:= ioutil.ReadAll(response.Body)
        if(err!=nil){
            fmt.Println(err)
        }
        json.Unmarshal(contents,&out)
        result,_:= json.Marshal(out)
        fmt.Println(string(result))
    }
}




func main() { 
  
    consistent := ConsistentHashingAlgo()      
    consistent.AddNode(NewNode(0, "127.0.0.1:3000"))
	consistent.AddNode(NewNode(1, "127.0.0.1:3001"))
	consistent.AddNode(NewNode(2, "127.0.0.1:3002")) 
    var cont string ="Y"
    for cont=="Y"{
    var choice int
	fmt.Println("***  REDIS Consistent Hashing Servers Running on Port 3000,3001 and 3002 ***\n")
    
	fmt.Println("1.PUT Values \n2.GET a specific Key Value pair \n3.GET all KeyValue Pairs\n4.GET REDIS port specific keyValues\n5.Display KeyValues on Different Redis Servers\n")
    
	fmt.Print("Enter your choice  :")
    fmt.Scanf("%d\n",&choice)
    switch choice{
        case 1:
            PutKeyValues(consistent)
            break
        case 2:
            fmt.Print("\n Enter Specific Key to search in Consistent Hashing Cache Servers:")
            var key string
            fmt.Scanf("%s\n",&key) 
            GetKeyValue(key,consistent)
            break
        case 3:
            GetKeyValues(consistent)
            break
        case 4:
            GetSpecificPort()
            break

        case 5:
            MappedOutput()
            break 

        default:
            break
        } 
		
    fmt.Println();
    fmt.Print("Continue ? (Y/N) :")
    fmt.Scanf("%s\n",&cont)
    }
}  
