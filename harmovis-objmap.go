package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	gosocketio "github.com/mtfelian/golang-socketio"
	"github.com/mtfelian/golang-socketio/transport"
	fleet "github.com/synerex/proto_fleet"
	geo "github.com/synerex/proto_geography"
	mqtt "github.com/synerex/proto_mqtt"
	pagent "github.com/synerex/proto_people_agent"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

// Harmoware Vis-Synerex wiht Layer extension provider provides map information to Web Service through socket.io.

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	assetDir        = flag.String("assetdir", "", "set Web client dir")
	mapbox          = flag.String("mapbox", "", "Set Mapbox access token")
	port            = flag.Int("port", 10080, "HarmoVis Ext Provider Listening Port")
	mu              = new(sync.Mutex)
	version         = "0.03"
	assetsDir       http.FileSystem
	ioserv          *gosocketio.Server
	sxServerAddress string
	mapboxToken     string
)

const (
	latBase = 35.181433
	lonBase = 136.906421
	xscale  = 9.109
	yscale  = 11.094
)

func toJSON(m map[string]interface{}, utime int64) string {
	s := fmt.Sprintf("{\"mtype\":%d,\"id\":%d,\"time\":%d,\"lat\":%f,\"lon\":%f,\"angle\":%f,\"speed\":%d}",
		0, int(m["vehicle_id"].(float64)), utime, m["coord"].([]interface{})[0].(float64), m["coord"].([]interface{})[1].(float64), m["angle"].(float64), int(m["speed"].(float64)))
	return s
}

func handleFleetMessage(sv *gosocketio.Server, param interface{}) {
	var bmap map[string]interface{}
	utime := time.Now().Unix()
	bmap = param.(map[string]interface{})
	for _, v := range bmap["vehicles"].([]interface{}) {
		m, _ := v.(map[string]interface{})
		s := toJSON(m, utime)
		sv.BroadcastToAll("event", s)
	}
}

func getFleetInfo(serv string, sv *gosocketio.Server, ch chan error) {
	fmt.Printf("Dial to [%s]\n", serv)
	sioClient, err := gosocketio.Dial(serv+"socket.io/?EIO=3&transport=websocket", transport.DefaultWebsocketTransport())
	if err != nil {
		log.Printf("SocketIO Dial error: %s", err)
		return
	}

	sioClient.On(gosocketio.OnConnection, func(c *gosocketio.Channel, param interface{}) {
		fmt.Println("Fleet-Provider socket.io connected ", c)
	})

	sioClient.On(gosocketio.OnDisconnection, func(c *gosocketio.Channel, param interface{}) {
		fmt.Println("Fleet-Provider socket.io disconnected ", c)
		ch <- fmt.Errorf("Disconnected!\n")
	})

	sioClient.On("vehicle_status", func(c *gosocketio.Channel, param interface{}) {
		handleFleetMessage(sv, param)
	})

}

func runFleetInfo(serv string, sv *gosocketio.Server) {
	ch := make(chan error)
	for {
		time.Sleep(3 * time.Second)
		getFleetInfo(serv, sv, ch)
		res := <-ch
		if res == nil {
			break
		}
	}
}

// assetsFileHandler for static Data
func assetsFileHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return
	}

	file := r.URL.Path
	//	log.Printf("Open File '%s'",file)
	if file == "/" {
		file = "/index.html"
	}
	f, err := assetsDir.Open(file)
	if err != nil {
		log.Printf("can't open file %s: %v\n", file, err)
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		log.Printf("can't open file %s: %v\n", file, err)
		return
	}
	http.ServeContent(w, r, file, fi.ModTime(), f)
}

func run_server() *gosocketio.Server {

	currentRoot, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	if *assetDir != "" {
		currentRoot = *assetDir
	}

	d := filepath.Join(currentRoot, "mclient", "build")

	assetsDir = http.Dir(d)
	log.Println("AssetDir:", assetsDir)

	assetsDir = http.Dir(d)
	server := gosocketio.NewServer()

	server.On(gosocketio.OnConnection, func(c *gosocketio.Channel) {
		// wait for a few milli seconds.
		log.Printf("Connected from %s as %s", c.IP(), c.Id())

	})

	server.On("get_mapbox_token", func(c *gosocketio.Channel) {
		log.Printf("Requested mapbox access token")
		mapboxToken = os.Getenv("MAPBOX_ACCESS_TOKEN")
		if *mapbox != "" {
			mapboxToken = *mapbox
		}
		c.Emit("mapbox_token", mapboxToken)
		log.Printf("mapbox-token transferred %s ", mapboxToken)
	})

	server.On(gosocketio.OnDisconnection, func(c *gosocketio.Channel) {
		log.Printf("Disconnected from %s as %s", c.IP(), c.Id())
	})

	return server
}

type MapMarker struct {
	mtype int32   `json:"mtype"`
	id    int32   `json:"id"`
	lat   float32 `json:"lat"`
	lon   float32 `json:"lon"`
	angle float32 `json:"angle"`
	speed int32   `json:"speed"`
}

type Position struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	Z float64 `json:"z"`
}

type Orientation struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	Z float64 `json:"z"`
	W float64 `json:"w"`
}

type Pose struct {
	Header struct {
		Seq   int `json:"seq"`
		Stamp struct {
			Secs  int `json:"secs"`
			Nsecs int `json:"nsecs"`
		} `json:"stamp"`
		FrameID string `json:"frame_id"`
	} `json:"header"`
	Pose struct {
		Pos Position    `json:"position"`
		Ori Orientation `json:"orientation"`
	} `json:"pose"`
}

func (m *MapMarker) GetJson() string {
	s := fmt.Sprintf("{\"mtype\":%d,\"id\":%d,\"lat\":%f,\"lon\":%f,\"angle\":%f,\"speed\":%d}",
		m.mtype, m.id, m.lat, m.lon, m.angle, m.speed)
	return s
}

func supplyRideCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	flt := &fleet.Fleet{}
	err := proto.Unmarshal(sp.Cdata.Entity, flt)
	if err == nil {
		mm := &MapMarker{
			mtype: int32(0),
			id:    flt.VehicleId,
			lat:   flt.Coord.Lat,
			lon:   flt.Coord.Lon,
			angle: flt.Angle,
			speed: flt.Speed,
		}
		//		jsondata, err := json.Marshal(*mm)
		//		fmt.Println("rcb",mm.GetJson())
		mu.Lock()
		ioserv.BroadcastToAll("event", mm.GetJson())
		mu.Unlock()
	}
}

func reconnectClient(client *sxutil.SXServiceClient) {
	mu.Lock() // first make client into nil
	if client.Client != nil {
		client.Client = nil
		log.Printf("Client reset \n")
	}
	mu.Unlock()
	time.Sleep(5 * time.Second) // wait 5 seconds to reconnect
	mu.Lock()
	if client.Client == nil {
		newClt := sxutil.GrpcConnectServer(sxServerAddress)
		if newClt != nil {
			log.Printf("Reconnect server [%s]\n", sxServerAddress)
			client.Client = newClt
		}
	} else { // someone may connect!
		log.Printf("Use reconnected server\n", sxServerAddress)
	}
	mu.Unlock()
}

func subscribeRideSupply(client *sxutil.SXServiceClient) {
	for {
		ctx := context.Background() //
		err := client.SubscribeSupply(ctx, supplyRideCallback)
		log.Printf("Error:Supply %s\n", err.Error())
		// we need to restart
		reconnectClient(client)
	}
}

func supplyGeoCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	switch sp.SupplyName {
	case "GeoJson":
		geo := &geo.Geo{}
		err := proto.Unmarshal(sp.Cdata.Entity, geo)
		if err == nil {
			strjs := string(geo.Data)
			log.Printf("Obtaining %s, id:%d, %s, len:%d ", geo.Type, geo.Id, geo.Label, len(strjs))
			log.Printf("Data '%s'", strjs)
			mu.Lock()
			ioserv.BroadcastToAll("geojson", strjs)
			mu.Unlock()
		}
	case "Lines":
		geo := &geo.Lines{}
		err := proto.Unmarshal(sp.Cdata.Entity, geo)
		if err == nil {

			jsonBytes, _ := json.Marshal(geo.Lines)
			//			log.Printf("Lines: %v", string(jsonBytes))

			mu.Lock()
			ioserv.BroadcastToAll("lines", string(jsonBytes))
			mu.Unlock()
		}
	case "ViewState":
		vs := &geo.ViewState{}
		err := proto.Unmarshal(sp.Cdata.Entity, vs)
		if err == nil {
			jsonBytes, _ := json.Marshal(vs)
			log.Printf("ViewState: %v", string(jsonBytes))

			mu.Lock()
			ioserv.BroadcastToAll("mapbox_token", mapboxToken)

			ioserv.BroadcastToAll("viewstate", string(jsonBytes))
			mu.Unlock()
		}
	case "ClearMoves":
		cms := &geo.ClearMoves{}
		err := proto.Unmarshal(sp.Cdata.Entity, cms)
		if err == nil {
			jsonBytes, _ := json.Marshal(cms)
			log.Printf("ClearMoves: %v", string(jsonBytes))

			mu.Lock()
			ioserv.BroadcastToAll("clearMoves", string(jsonBytes))
			mu.Unlock()
		}
	case "Pitch":
		cms := &geo.Pitch{}
		err := proto.Unmarshal(sp.Cdata.Entity, cms)
		if err == nil {
			jsonBytes, _ := json.Marshal(cms)
			log.Printf("Pitch: %v", string(jsonBytes))

			mu.Lock()
			ioserv.BroadcastToAll("pitch", string(jsonBytes))
			mu.Unlock()
		}
	case "Bearing":
		cms := &geo.Bearing{}
		err := proto.Unmarshal(sp.Cdata.Entity, cms)
		if err == nil {
			jsonBytes, _ := json.Marshal(cms)
			log.Printf("Bearing: %v", string(jsonBytes))

			mu.Lock()
			ioserv.BroadcastToAll("bearing", string(jsonBytes))
			mu.Unlock()
		}
	}

}

func subscribeGeoSupply(client *sxutil.SXServiceClient) {
	for {
		ctx := context.Background() //
		err := client.SubscribeSupply(ctx, supplyGeoCallback)
		log.Printf("Error:Supply %s\n", err.Error())
		// we need to restart
		reconnectClient(client)

	}
}

func supplyMQTTCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	mqttRCD := mqtt.MQTTRecord{}
	err := proto.Unmarshal(sp.Cdata.Entity, &mqttRCD)
	if err == nil {
		var rid int32
		if strings.HasPrefix(mqttRCD.Topic, "pos/robot") {
			n, nerr := fmt.Sscanf(mqttRCD.Topic, "pos/robot/%d/pse", &rid)
			if n == 1 && nerr == nil { // robot pose into location
				var pose Pose
				jerr := json.Unmarshal(mqttRCD.Record, &pose)
				if jerr == nil {
					mm := &MapMarker{
						mtype: int32(0),
						id:    rid,
						lat:   float32(latBase + 0.001*(pose.Pose.Pos.Y/yscale)),
						lon:   float32(lonBase + 0.001*(pose.Pose.Pos.X/xscale)),
						angle: float32(pose.Pose.Ori.Z),
						speed: 1,
					}
					log.Printf("Map:%s", mm.GetJson())
					mu.Lock()
					ioserv.BroadcastToAll("event", mm.GetJson())
					mu.Unlock()
				} else {
					log.Printf("Unmarshal MQTT robot record error! %v %v", jerr, mqttRCD)
				}
			}
		} else if strings.HasPrefix(mqttRCD.Topic, "pos/human/all") {
			var poses []Pose
			jerr := json.Unmarshal(mqttRCD.Record, &poses)
			if jerr == nil {
				agts := make([]*pagent.PAgent, len(poses))
				for i, ps := range poses {
					fid, _ := strconv.Atoi(ps.Header.FrameID)
					agts[i] = &pagent.PAgent{
						Id:    int32(fid),
						Point: []float64{lonBase + 0.001*(ps.Pose.Pos.X/xscale), latBase + 0.001*(ps.Pose.Pos.Y/yscale)},
					}
				}
				agents := pagent.PAgents{
					Agents: agts,
				}
				seconds := time.Now().Unix()
				//				sp.Ts.GetSeconds()
				//				nanos := sp.Ts.GetNanos()
				jsonBytes, _ := json.Marshal(agents)
				jstr := fmt.Sprintf("{ \"ts\": %d.%03d, \"dt\": %s}", seconds, 0, string(jsonBytes))
				log.Printf("Agents:%s", jstr)
				mu.Lock()
				ioserv.BroadcastToAll("agents", jstr)
				mu.Unlock()
			} else {
				log.Printf("Unmarshal MQTT human record error! %v %v", jerr, mqttRCD)
			}

		}

	} else {
		log.Printf("Unmarshal Proto error! %v %v", err, mqttRCD)
	}
}

func subscribeMQTTSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	for {
		err := client.SubscribeSupply(ctx, supplyMQTTCallback)
		log.Printf("Error:Suply %s\n", err.Error())
		// we need torestart
		reconnectClient(client)
	}
}

func supplyPAgentCallback(cl *sxutil.SXServiceClient, sp *api.Supply) {
	switch sp.SupplyName {
	case "Agents":
		agents := &pagent.PAgents{}
		err := proto.Unmarshal(sp.Cdata.Entity, agents)
		if err == nil {
			seconds := sp.Ts.GetSeconds()
			nanos := sp.Ts.GetNanos()
			jsonBytes, _ := json.Marshal(agents)
			jstr := fmt.Sprintf("{ \"ts\": %d.%03d, \"dt\": %s}", seconds, int(nanos/1000000), string(jsonBytes))
			//				log.Printf("Lines: %v", jstr)
			mu.Lock()
			ioserv.BroadcastToAll("agents", jstr)
			mu.Unlock()
		}
	}

}

func subscribePAgentSupply(client *sxutil.SXServiceClient) {
	for {
		ctx := context.Background() //
		err := client.SubscribeSupply(ctx, supplyPAgentCallback)
		log.Printf("Error:Supply %s\n", err.Error())
		// we need to restart
		reconnectClient(client)
	}
}

/*
func supplyPTCallback(clt *sxutil.SXServiceClient, sp *api.upply) {
//	pt := sp.GetArg_TService()
	if pt != nil { // et Fleet supplu
		m := &MapMarker{
			mtype: p.VehicleType, // depends on type of GTFS: 1 for Subway, 2, for Rail, 3 for bus
			id:    pt.VehicleId,
			lat:   flot32(pt.CurrentLocation.GetPoint().Latitude),
		lon:   float32(pt.CurrentLocation.GetPoint().Longitude),
		angle: pt.Angle,
		speed: pt.Speed,
		}
		mu.Lock()
		ioserv.BroadcastToAll("event", mm.GetJson())
		mu.Unlock()

}

func subscribePTSupplyclient *sxutil.SXServiceClient) {
	ctx : context.Background() //
	err := client.SubscribeSupply(ctx, supplyPTCallback)
	log.Printf("Error:Supply %s\",err.Error())
}
*/

func monitorStatus() {
	for {
		sxutil.SetNodeStatus(int32(runtime.NumGoroutine()), "HV")
		time.Sleep(time.Second * 3)
	}
}

func main() {
	flag.Parse()

	channelTypes := []uint32{pbase.RIDE_SHARE, pbase.PEOPLE_AGENT_SVC, pbase.GEOGRAPHIC_SVC, pbase.MQTT_GATEWAY_SVC}
	var rerr error
	sxServerAddress, rerr = sxutil.RegisterNode(*nodesrv, "HarmoVisObjMap", channelTypes, nil)
	if rerr != nil {
		log.Fatal("Can't register node ", rerr)
	}
	log.Printf("Connectin SynerexServer at [%s]\n", sxServerAddress)

	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	wg := sync.WaitGroup{} // for syncing other goroutines

	ioserv = run_server()
	fmt.Printf("Running HarmoVisObjMap Server.\n")
	if ioserv == nil {
		os.Exit(1)
	}

	client := sxutil.GrpcConnectServer(sxServerAddress) // if there is server address change, we should do it!

	argJSON := fmt.Sprintf("{Client:Map:RIDE")
	rideClient := sxutil.NewSXServiceClient(client, pbase.RIDE_SHARE, argJSON)

	argJSON2 := fmt.Sprintf("{Client:Map:PAGENT}")
	pa_client := sxutil.NewSXServiceClient(client, pbase.PEOPLE_AGENT_SVC, argJSON2)

	argJSON3 := fmt.Sprintf("{Client:Mp:Geo}")
	geo_client := sxutil.NewSXServiceClient(client, pbase.GEOGRAPHIC_SVC, argJSON3)

	argJSON4 := fmt.Sprintf("{Client:MQTT}")
	mqtt_client := sxutil.NewSXServiceClient(client, pbase.MQTT_GATEWAY_SVC, argJSON4)

	wg.Add(1)
	go subscribeRideSupply(rideClient)

	go subscribePAgentSupply(pa_client)

	go subscribeGeoSupply(geo_client)

	go subscribeMQTTSupply(mqtt_client)

	go monitorStatus() // keep status

	serveMux := http.NewServeMux()

	serveMux.Handle("/socket.io/", ioserv)
	serveMux.HandleFunc("/", assetsFileHandler)

	log.Printf("Starting Harmoware-VIS ObjMap Provider %s  on port %d", version, *port)
	err := http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", *port), serveMux)
	if err != nil {
		log.Fatal(err)
	}

	wg.Wait()

}