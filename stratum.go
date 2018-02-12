package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	MaxReqSize = 1024
)

var wnLock sync.Mutex
var workNum = 1000

func getWorkNum() string {
	wnLock.Lock()
	defer wnLock.Unlock()
	workNum++
	return strconv.Itoa(workNum)
}

type StratumCfg struct {
	Enabled bool   //`json:"enabled"`
	Listen  string //`json:"listen"`
	Timeout string //`json:"timeout"`
	MaxConn int    //`json:"maxConn"`
}

type StratumServer struct {
	cfg          *StratumCfg
	pushWorkChan chan *MiningWork
	workReqChan  chan chan *MiningWork
	submitChan   chan *[]byte
	//	blockTemplate      atomic.Value
	//	upstream           int32
	//	upstreams          []*rpc.RPCClient
	//	backend            *storage.RedisClient
	//	diff               string
	//	policy             *policy.PolicyServer
	//	hashrateExpiration time.Duration
	//	failsCount         int64
	//
	//	// Stratum
	sessionsMu sync.RWMutex
	sessions   map[*Session]struct{}
	timeout    time.Duration
}

func NewStratumServer(cfg *StratumCfg, pushChan chan *MiningWork, reqChan chan chan *MiningWork,
	submitChan chan *[]byte) *StratumServer {

	s := new(StratumServer)
	s.cfg = cfg
	s.pushWorkChan = pushChan
	s.submitChan = submitChan
	s.workReqChan = reqChan

	s.sessions = make(map[*Session]struct{})

	return s
}

type Session struct {
	ip  string
	enc *json.Encoder

	// Stratum
	sync.Mutex
	conn  *net.TCPConn
	login string
}

func (s *StratumServer) start() {
	go s.listenTCP()

	for {
		select {
		case mWork := <-s.pushWorkChan:
			s.broadcastNewJobs(mWork.Header)
		}
	}

}

func (s *StratumServer) listenTCP() {
	timeout := MustParseDuration(s.cfg.Timeout)
	s.timeout = timeout

	addr, err := net.ResolveTCPAddr("tcp", s.cfg.Listen)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	server, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Fatalf("Error: %v, %v", err, addr)
	}
	defer server.Close()

	log.Printf("Stratum listening on %s", s.cfg.Listen)
	var accept = make(chan int, s.cfg.MaxConn)
	n := 0

	for {
		conn, err := server.AcceptTCP()
		if err != nil {
			continue
		}
		conn.SetKeepAlive(true)

		ip, _, _ := net.SplitHostPort(conn.RemoteAddr().String())

		n += 1
		cs := &Session{conn: conn, ip: ip}

		accept <- n
		go func(cs *Session) {
			err = s.handleTCPClient(cs)
			if err != nil {
				s.removeSession(cs)
				conn.Close()
			}
			<-accept
		}(cs)
	}
}

func (s *StratumServer) handleTCPClient(cs *Session) error {
	cs.enc = json.NewEncoder(cs.conn)
	connbuff := bufio.NewReaderSize(cs.conn, MaxReqSize)
	s.setDeadline(cs.conn)

	for {
		data, isPrefix, err := connbuff.ReadLine()
		if isPrefix {
			log.Printf("Socket flood detected from %s", cs.ip)
			return err
		} else if err == io.EOF {
			log.Printf("Client %s disconnected", cs.ip)
			s.removeSession(cs)
			break
		} else if err != nil {
			log.Printf("Error reading from socket: %v", err)
			return err
		}

		if len(data) > 1 {
			log.Printf("received request from  %s: %v \n", cs.ip, data)
			var req StratumReq
			err = json.Unmarshal(data, &req)
			if err != nil {
				log.Printf("Malformed stratum request from %s: %v", cs.ip, err)
				return err
			}
			s.setDeadline(cs.conn)
			err = cs.handleTCPMessage(s, &req)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *StratumServer) handleUnknownRPC(cs *Session, m string) *ErrorReply {
	log.Printf("Unknown request method %s from %s", m, cs.ip)
	return &ErrorReply{Code: -3, Message: "Method not found"}
}

func (cs *Session) handleTCPMessage(s *StratumServer, req *StratumReq) error {
	// Handle RPC methods
	switch req.Method {
	case "mining.subscribe":
		var params []string
		err := json.Unmarshal(*req.Params, &params)
		if err != nil {
			log.Println("Malformed stratum request params from ", cs.ip)
			return err
		}
		//这里先写死subscribe的返回
		//{"result":[[["mining.notify","02009857803447bf"],["mining.set_difficulty","02009857803447bf2"]],"02009857",4],"id":1,"error":null}
		set_not := []string{"mining.notify", "02009857803447bf"}
		set_dif := []string{"mining.set_difficulty", "02009857803447bf2"}
		set := [][]string{set_not, set_dif}
		ret := make([]interface{}, 3, 3)
		ret = append(ret, set, "02009857", 4)

		err = cs.sendDifficulty()
		if err != nil {
			log.Println("Send difficulty faild ", err)
			return err
		}

		mwChan := make(chan *MiningWork)
		s.workReqChan <- mwChan
		mw := <-mwChan

		err = cs.sendNotify(mw.Header)
		if err != nil {
			log.Println("Send notify faild ", err)
			return err
		}
		return cs.sendTCPResult(req.Id, ret)
	case "mining.authorize":
		var params []string
		err := json.Unmarshal(*req.Params, &params)
		if err != nil {
			log.Println("Malformed stratum request params from ", cs.ip)
			return err
		}
		return cs.sendTCPResult(req.Id, true)
	case "mining.submit":
		var params []string
		err := json.Unmarshal(*req.Params, &params)
		if err != nil || params == nil || len(params) != 3 {
			log.Println("Malformed stratum request params from ", cs.ip)
			return err
		}
		newHeader := []byte(params[2])
		s.submitChan <- &newHeader
		return cs.sendTCPResult(req.Id, true)
	default:
		errReply := s.handleUnknownRPC(cs, req.Method)
		return cs.sendTCPError(req.Id, errReply)
	}
}

func (cs *Session) sendDifficulty() error {
	//	p := make([]inter)
	commandDifficulty := JSONPushMessage{Method: "mining.set_difficulty", Params: []interface{}{7.9998779296875284}}
	return cs.enc.Encode(&commandDifficulty)
}

func (cs *Session) sendNotify(header []byte) error {
	wn := getWorkNum()

	paras := make([]interface{}, 4, 4)
	paras = append(paras, wn, string(header), "0x000000001fffffffffffffffffffffffffffffffffffffffffffffffffffffff", false)
	commandNotify := JSONPushMessage{Method: "mining.notify", Params: paras}

	return cs.enc.Encode(&commandNotify)
}

func (cs *Session) sendTCPResult(id *json.RawMessage, result interface{}) error {
	cs.Lock()
	defer cs.Unlock()

	message := JSONRpcResp{Error: nil, Result: result}
	return cs.enc.Encode(&message)
}

func (cs *Session) pushNewJob(header []byte) error {
	cs.Lock()
	defer cs.Unlock()
	return cs.sendNotify(header)
}

func (cs *Session) sendTCPError(id *json.RawMessage, reply *ErrorReply) error {
	cs.Lock()
	defer cs.Unlock()

	message := JSONRpcResp{Id: id, Result: false, Error: reply}
	err := cs.enc.Encode(&message)
	if err != nil {
		return err
	}
	return errors.New(reply.Message)
}

func (self *StratumServer) setDeadline(conn *net.TCPConn) {
	conn.SetDeadline(time.Now().Add(self.timeout))
}

func (s *StratumServer) registerSession(cs *Session) {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	s.sessions[cs] = struct{}{}
}

func (s *StratumServer) removeSession(cs *Session) {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	delete(s.sessions, cs)
}

func (s *StratumServer) broadcastNewJobs(header []byte) {
	s.sessionsMu.RLock()
	defer s.sessionsMu.RUnlock()

	count := len(s.sessions)
	log.Printf("Broadcasting new job to %v stratum miners", count)

	start := time.Now()
	bcast := make(chan int, 1024)
	n := 0

	for m, _ := range s.sessions {
		n++
		bcast <- n

		go func(cs *Session) {
			err := cs.pushNewJob(header)
			<-bcast
			if err != nil {
				log.Printf("Job transmit error to %v@%v: %v", cs.login, cs.ip, err)
				s.removeSession(cs)
			} else {
				s.setDeadline(cs.conn)
			}
		}(m)
	}
	log.Printf("Jobs broadcast finished %s", time.Since(start))
}
