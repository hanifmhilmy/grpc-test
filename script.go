package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	aggpb "github.com/tokopedia/grpc/product-agg/proto"

	_ "github.com/lib/pq"
)

const (
	host   = "<HOSTDB>"
	port   = 5432
	dbName = "<DBNAME>"

	username = ""
	password = ""
)

type client struct {
	agg *aggpb.Client
}

type Bundle struct {
	BundleID         int64     `json:"bundle_id"`
	ShopID           int64     `json:"shopId"`
	Status           int       `json:"status"`
	CreateTime       time.Time `json:"-"`
	CreateTimeParsed string    `json:"createdTime"`

	CountSold          int64   `json:"sold"`
	ProductIDs         []int64 `json:"productIds"`
	DiscountAmount     int64   `json:"discountAmount"`
	DiscountPercentage int64   `json:"discountPercentage"`

	MinOrder int64 `json:"-"`
}

type (
	BundlingAutomation struct {
		Index string       `json:"index"`
		Data  []InsertData `json:"data"`
	}
	InsertData struct {
		BundleId int64     `json:"doc_id"`
		DocData  InsideDoc `json:"doc"`
	}
	InsideDoc struct {
		BundleId           int64   `json:"bundleId"`
		CreatedTime        string  `json:"createdTime"`
		ShopId             int64   `json:"shopId"`
		Status             string  `json:"status"`
		DiscountPercentage float64 `json:"discountPercentage"`
		Sold               int     `json:"sold"`
		DiscountAmount     int64   `json:"discountAmount"`
		ProductIDs         []int64 `json:"productIds"`
	}

	Response struct {
		Data     data `json:"data"`
		Testdata test `json:"header"`
	}

	data struct {
		Item []items `json:"items"`
	}
	items struct {
		Index        string `json:"index"`
		BundleID     string `json:"doc_id"`
		ErrorType    string `json:"error_type"`
		Errormessage string `json:"error_message"`
		StatusCode   int64  `json:"status_code"`
	}
	test struct {
		ClientId     string  `json:"client_id"`
		Processtime  float64 `json:"process_time"`
		Responsecode int64   `json:"response_code"`
		Totaldata    int64   `json:"total_data"`
		Totaltext    string  `json:"total_data_text"`
	}
)

// Pool is a worker group that runs a number of tasks at a
// configured concurrency.
type Pool struct {
	Tasks []*Task

	concurrency int
	tasksChan   chan *Task
	wg          sync.WaitGroup
}

// NewPool initializes a new pool with the given tasks and
// at the given concurrency.
func NewPool(tasks []*Task, concurrency int) *Pool {
	return &Pool{
		Tasks:       tasks,
		concurrency: concurrency,
		tasksChan:   make(chan *Task),
	}
}

// Run runs all work within the pool and blocks until it's
// finished.
func (p *Pool) Run() {
	for i := 0; i < p.concurrency; i++ {
		go p.work()
	}

	p.wg.Add(len(p.Tasks))
	for _, task := range p.Tasks {
		p.tasksChan <- task
	}

	// all workers return
	close(p.tasksChan)

	p.wg.Wait()
}

// The work loop for any single goroutine.
func (p *Pool) work() {
	for task := range p.tasksChan {
		task.Run(&p.wg)
	}
}

// Task encapsulates a work item that should go in a work
// pool.
type Task struct {
	// Err holds an error that occurred during a task. Its
	// result is only meaningful after Run has been called
	// for the pool that holds it.
	Err error

	f func() error
}

// NewTask initializes a new task based on a given work
// function.
func NewTask(f func() error) *Task {
	return &Task{f: f}
}

// Run runs a Task and does appropriate accounting via a
// given sync.WorkGroup.
func (t *Task) Run(wg *sync.WaitGroup) {
	t.Err = t.f()
	wg.Done()
}

var httpCli = &http.Client{}

func main() {
	f, err := os.OpenFile("report.log", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)

	startID := flag.Int64("start", 0, "start bundle id")
	limit := flag.Int("limit", 1000, "limit row from db")
	flag.Parse()

	connString := fmt.Sprintf("host=%s port=%d user=%s "+"password=%s dbname=%s sslmode=disable",
		host, port, username, password, dbName)

	db, err := sql.Open("postgres", connString)
	if err != nil {
		log.Println("❌ Error opening db connection", err)
		panic(err)
	}
	defer db.Close()

	cli, err := initCli()
	if err != nil {
		panic(err)
	}

	tasks := make([]*Task, 0)
	for {
		rows, err := db.Query("SELECT shop_id, bundle_id, bundle_status, create_time FROM ws_bundle WHERE bundle_id > $1 ORDER BY bundle_id ASC LIMIT $2", *startID, *limit)
		if err != nil {
			log.Println(err)
			break
		}

		bundleList := make([]Bundle, 0)
		bundleMapDetails := make(map[int64]Bundle)
		for rows.Next() {
			var bundleData Bundle

			err := rows.Scan(&bundleData.ShopID, &bundleData.BundleID, &bundleData.Status, &bundleData.CreateTime)
			if err != nil {
				continue
			}
			bundleData.CreateTimeParsed = bundleData.CreateTime.Format("2006-01-02 15:04:05")

			bundleMapDetails[bundleData.BundleID] = bundleData
			bundleList = append(bundleList, bundleData)
		}

		if rows.Err() != nil {
			log.Println("found err during reading/closing rows", rows.Err())
		}

		chunkedBundleList := chunkSliceOfBundle(bundleList, 10)
		for index := range chunkedBundleList {
			chunked := chunkedBundleList[index]

			task := NewTask(func() error {
				aggres, err := getBundleInfo(cli, chunked)
				if err != nil {
					log.Println("got failed get bundle details", chunked)
					return err
				}
				payloadData := make([]InsideDoc, 0, len(aggres.GetBundleInfo()))
				for _, data := range aggres.GetBundleInfo() {
					bd, ok := bundleMapDetails[data.GetBundleID()]
					if !ok {
						log.Println("strangely not found in map", data.GetBundleID())
						continue
					}
					payload := InsideDoc{
						BundleId:    bd.BundleID,
						ShopId:      bd.ShopID,
						CreatedTime: bd.CreateTimeParsed,
						Status: func() string {
							switch data.GetStatus() {
							case aggpb.BundleStatus_UPCOMING:
								return "-1"
							case aggpb.BundleStatus_ACTIVE:
								return "1"
							case aggpb.BundleStatus_CANCELLED:
								return "3"
							default:
								return "2"
							}
						}(),
						Sold: int(data.GetBundleStats().GetSold()),
					}
					oriPrice, disc := float64(0), float64(0)
					for _, item := range data.GetBundleItem() {
						if item.GetProductStatus() == "INACTIVE" {
							continue
						}
						if len(item.GetChild()) == 0 {
							payload.ProductIDs = append(payload.ProductIDs, item.GetProductID())

							oriPrice += item.GetOriginalPrice() * float64(item.GetMinOrder())
							disc += (item.GetOriginalPrice() - item.GetBundlePrice()) * float64(item.GetMinOrder())
							continue
						}

						for _, ch := range item.GetChild() {
							payload.ProductIDs = append(payload.ProductIDs, ch.GetProductID())
						}

						// variant case
						priceItemVar := item.GetChild()
						sort.SliceStable(priceItemVar, func(i, j int) bool {
							return priceItemVar[i].GetBundlePrice() < priceItemVar[j].GetBundlePrice()
						})

						oriPrice += priceItemVar[0].GetOriginalPrice() * float64(priceItemVar[0].GetMinOrder())
						disc += (priceItemVar[0].GetOriginalPrice() - priceItemVar[0].GetBundlePrice()) * float64(priceItemVar[0].GetMinOrder())
					}
					percent := ((oriPrice - disc) / oriPrice) * 100
					if percent < 0 {
						percent = 0
					}
					payload.DiscountPercentage = percent
					payload.DiscountAmount = int64(disc)

					payloadData = append(payloadData, payload)
				}

				req := make([]InsertData, 0, len(payloadData))
				for i := range payloadData {
					req = append(req, InsertData{
						BundleId: payloadData[i].BundleId,
						DocData:  payloadData[i],
					})
				}

				err = ingestDataToES(BundlingAutomation{
					Index: "bundling_automation",
					Data:  req,
				})
				if err != nil {
					return err
				}
				return nil
			})
			tasks = append(tasks, task)
		}
		if len(bundleList) == 0 {
			break
		}
		*startID = bundleList[len(bundleList)-1].BundleID
	}

	pool := NewPool(tasks, 5)
	pool.Run()

	var numErrors int
	for _, task := range pool.Tasks {
		if task.Err != nil {
			log.Println("Err", task.Err)
			numErrors++
		}
		if numErrors > 10 {
			log.Println("too many errors")
			break
		}
	}
}

func chunkSliceOfBundle(arr []Bundle, size int) (chunked [][]Bundle) {
	lenSlice := len(arr)
	chunkSize := size
	for i := 0; i < lenSlice; i += chunkSize {
		end := i + chunkSize
		if end > lenSlice {
			end = lenSlice
		}
		chunked = append(chunked, arr[i:end])
	}
	return chunked
}

func getBundleInfo(cli client, payload []Bundle) (*aggpb.BundleInfoResponse, error) {
	req := make([]*aggpb.Bundle, 0, len(payload))
	// construct payload
	for _, p := range payload {
		req = append(req, &aggpb.Bundle{
			ID: p.BundleID,
		})
	}

	return cli.agg.GetBundleInfo(context.Background(), &aggpb.GetBundleInfoRequest{
		Bundles: req,
		RequestData: &aggpb.RequestData{
			BundleStats: true,
			InventoryDetail: &aggpb.InventoryDetail{
				Required: true,
			},
		},
		Source: &aggpb.Source{
			Squad:   "bundle-es-ingestion",
			Usecase: "import-bundle-to-es",
		},
	})
}

func ingestDataToES(payload BundlingAutomation) error {
	if len(payload.Data) == 0 {
		return nil
	}

	responseData := Response{}
	bytesRepresentation, err := json.Marshal(payload)
	if err != nil {
		log.Fatalln("error while marshaling", err)
	}
	url := "<SEARCH API URL>"
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(bytesRepresentation))
	if err != nil {
		log.Fatalln("error while creating new Request", err)
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := httpCli.Do(req)
	if err != nil {
		log.Printf("Error while posting the request : %+v\n", err)
		return err
	} else if response.StatusCode != http.StatusOK {
		return errors.New("status not ok, check the request")
	}
	defer response.Body.Close()

	httpCli.CloseIdleConnections()
	bodyBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}
	err = json.Unmarshal(bodyBytes, &responseData)
	if err != nil {
		log.Printf("Error while unmarshalling response from BulkInsert endpoint: %v", err)
		return err
	}
	return nil
}

func initCli() (client, error) {
	hostAgg := "<AGGREGATORHOST GRPC>"

	ctxAgg, _ := context.WithTimeout(context.TODO(), 5*time.Second)
	cliAgg, err := aggpb.NewStandardClient(ctxAgg, hostAgg)
	if err != nil {
		return client{}, err
	}

	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 100
	t.MaxConnsPerHost = 100
	t.MaxIdleConnsPerHost = 100

	httpCli = &http.Client{
		Timeout:   10 * time.Second,
		Transport: t,
	}

	return client{
		agg: cliAgg,
	}, nil
}
