/************************************************************************************
* Copyright (c) 2017,
* All rights reserved.
*
* 文件名称：UploaddataToES
* 文件标识：Json数据上传至ES数据库
* 摘    要：
*
*   作者            日期            版本            描述
*   zoufawei	    2017-01-15     1.0
************************************************************************************/
// UploaddataToES project main.go
package main

import (
	"fmt"
	"net/http"

	"flag"
	"log"
	"os"
	"time"

	"runtime"

	"github.com/gin-gonic/gin"
	"github.com/larspensjo/config"
	"golang.org/x/net/context"
	"gopkg.in/olivere/elastic.v5"
)

// 避免没有引用fmt的编译错误
var _ = fmt.Println

func main() {
	//初始化日志文件
	InitLogger()
	//读取配置文件
	readConfigInfo()
	//初始化ES客户端
	InitES()
	//初始化中间件
	InitMiddleware()

}

var logger *log.Logger
var loggerTail string

func InitLogger() {
	loggerTail = "\r\n"
	//time.Now().Format("2006-01-02 15:04:05")
	file, err := os.Create("main.log")
	if err != nil {
		log.Fatalln("fail to create test.log file!")
	}
	logger = log.New(file, "", log.LstdFlags|log.Lshortfile|log.Lmicroseconds)
}

//读取配置文件域 开始
var (
	configFile = flag.String("configfile", "config.ini", "General configuration file")
)

//list
var ConfigInfo = make(map[string]string) //配置信息map
var g_esip string                        //es ip
var g_esport string                      //es 端口
var g_debug string                       //运行模式
var g_listenport string                  //监听端口

func readConfigInfo() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	//set config file std
	cfg, err := config.ReadDefault(*configFile)
	if err != nil {
		log.Fatalf("Fail to find", *configFile, err)
	}
	//set config file std End

	//ElasticsearchUrl from the configuration
	if cfg.HasSection("ElasticsearchUrl") {
		section, err := cfg.SectionOptions("ElasticsearchUrl")
		if err == nil {
			for _, v := range section {
				options, err := cfg.String("ElasticsearchUrl", v)
				if err == nil {
					ConfigInfo[v] = options
				}
			}
		}
	}
	g_esip = ConfigInfo["ip"]
	g_esport = ConfigInfo["port"]

	//Baseset from the configuration
	if cfg.HasSection("BaseSet") {
		section, err := cfg.SectionOptions("BaseSet")
		if err == nil {
			for _, v := range section {
				options, err := cfg.String("BaseSet", v)
				if err == nil {
					ConfigInfo[v] = options
				}
			}
		}
	}
	g_debug = ConfigInfo["debug"]
	g_listenport = ConfigInfo["ListenPort"]

}

//读取配置文件域 结束

type ques struct {
	qid   string
	title string
	added string
	orgid string
}

type Ques2 struct {
	Content string
}

func Middleware(c *gin.Context) {
	logger.Printf("------ The Middleware for upload data to ES ------" + loggerTail)
}

//调用接口处理函数
func upload2esHandler(c *gin.Context) {
	logger.Printf("上传接口处理函数被调用" + loggerTail)
	DealData(c)

	return
}

//初始化中间件
func InitMiddleware() {
	//全局设置环境，此为开发环境gin.DebugMode，线上环境为gin.ReleaseMode
	if g_debug == "true" {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	//获得路由实例
	router := gin.Default()
	//添加中间件
	router.Use(Middleware)
	//注册接口
	router.POST("/upload2es", upload2esHandler)
	//监听端口
	http.ListenAndServe(":"+g_listenport, router)
}

var client *elastic.Client //es客户端
var url string             //es url

//初始化es客户端
func InitES() {
	url = "http://" + g_esip + ":" + g_esport
	var err error
	// 创建一个客户端连接到 http://172.19.1.114:9200
	client, err = elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetSniff(false),
		elastic.SetHealthcheckInterval(10*time.Second),
		elastic.SetMaxRetries(5),
		elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)),
		elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags)))
	if err != nil {
		logger.Printf("初始化客户端失败" + loggerTail)
		panic(err)
	}
}

//处理数据
func DealData(c *gin.Context) {
	//数据接收
	data := c.Request.FormValue("data")
	fmt.Println("recv data：", data)
	//数据处理
	// Ping es 服务器并获取es版本号
	info, code, err := client.Ping(url).Do(context.Background())
	if err != nil {
		// Handle error
		logger.Printf("Ping es 服务器并获取es版本号失败" + loggerTail)
		panic(err)
	}
	fmt.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)

	// 获取 es 版本号
	esversion, err := client.ElasticsearchVersion(url)
	if err != nil {
		// Handle error
		logger.Printf("获取 es 版本号失败" + loggerTail)
		panic(err)
	}
	fmt.Printf("Elasticsearch version %s\n", esversion)

	// 判断 es 中的index是否存在
	exists, err := client.IndexExists("fklj").Do(context.Background())
	if err != nil {
		// Handle error
		logger.Printf("判断 es 中的index是否存在失败" + loggerTail)
		panic(err)
	}
	if !exists { //index 不存在
		// Index does not exist yet.
		// Create a new index.
		mapping := `
{
	"settings":{
		"number_of_shards":1,
		"number_of_replicas":0
	},
	"mappings":{
		"_default_": {
			"_all": {
				"enabled": true
			}
		},
		"tweet":{
			"properties":{
				"user":{
					"type":"keyword"
				},
				"message":{
					"type":"text",
					"store": true,
					"fielddata": true
				},
            "retweets":{
                "type":"long"
            },
				"tags":{
					"type":"keyword"
				},
				"location":{
					"type":"geo_point"
				},
				"suggest_field":{
					"type":"completion"
				}
			}
		}
	}
}
`
		//创建新的index
		createIndex, err := client.CreateIndex("twitter").Body(mapping).Do(context.Background())
		if err != nil {
			// Handle error
			logger.Printf("创建新的index失败" + loggerTail)
			panic(err)
		}
		if !createIndex.Acknowledged {
			// Not acknowledged
			logger.Printf("Not acknowledged" + loggerTail)
			panic(err)
		}
	} else { //index 存在
		// Index a second tweet (by string)
		tweet := data
		put2, err := client.Index().
			Index("fklj").
			Type("collect_data").
			BodyString(tweet).
			Do(context.Background()) //将文档写入到es
		if err != nil {
			// Handle error
			logger.Printf("将文档写入到es失败" + loggerTail)
			panic(err)
		} else {
			logger.Printf("将文档写入到es成功" + loggerTail)
		}
		fmt.Printf("Indexed tweet %s to index %s, type %s\n", put2.Id, put2.Index, put2.Type)

	}

	// 刷新一下，确保文档被写入
	_, err = client.Flush().Index("fklj").Do(context.Background())
	if err != nil {
		logger.Printf("client.Flush()失败" + loggerTail)
		panic(err)
	}

	// 测试查询
	termQuery := elastic.NewTermQuery("id", "23010719710620121X20161214161845924")
	searchResult, err := client.Search().
		Index("fklj").           // search in index "twitter"
		Query(termQuery).        // specify the query
		From(0).Size(10).        // take documents 0-9
		Pretty(true).            // pretty print request and response JSON
		Do(context.Background()) // execute
	if err != nil {
		// Handle error
		logger.Printf("测试查询失败" + loggerTail)
		panic(err)
	}
	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)

}
