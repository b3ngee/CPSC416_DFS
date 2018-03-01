package main

import (
	"bytes"
	"encoding/json"
	"net"
	"net/rpc"
	"os"
	"time"

	"./dfslib"
)

// Unique ID for each NEW client that mounts
var UID int = 1

// Client Record: UID mapped to ClientInfo struct
var clientLedger = map[int](ClientInfo){}

// Connected Client Record: ClientUID mapped to connection status (true = connected, false = disconnected)
var connectedClientLedger = map[int](ConnectionStatus){}

// File Record: File mapped to list of clients with file
var fileToClientLedger = map[string]([]int){}

// File Version Record: File mapped to 256-byte array with array of clients of latest chunk
// { "helloWorld.dfs" : { 0   : [version][clients]int }
// { "textFile.dfs"   : { 255 : [version][clients]int }
var fileToVersionLedger = map[string](map[int]([][]int)){}

// File Occupancy Record: File mapped to occupancy (mode)
var fileOccupancyLedger = map[string](Occupant){}

// Client File Record: Records which files the client current holds (used to quickly release files when client d/c)
var clientToFileLedger = map[int]([]string){}

type ClientInfo struct {
	UID      int
	IP       string
	Port     string
	FilePath string
	Files    []string
}

type GlobalSearchRequest struct {
	UID      int
	FileName string
}

type GlobalSearchResponse struct {
	FileExists bool
}

type OpenRequest struct {
	UID      int
	IP       string
	Port     string
	FileName string
	FilePath string
	FileMode dfslib.FileMode
}

type OpenResponse struct {
	OpenWriteConflictError bool
	FileUnavailableError   bool
	Data                   map[int]([]byte)
}

type CloseRequest struct {
	UID      int
	FileName string
}

type ReadRequest struct {
	UID      int
	ChunkNum int
	FileName string
	FilePath string
}

type ReadResponse struct {
	ChunkUnavailableError bool
	DisconnectedError     bool
	Data                  dfslib.Chunk
}

type WriteRequest struct {
	UID      int
	ChunkNum int
	FileName string
}

type UpdateRequest struct {
	UID      int
	FileName string
	FileMode dfslib.FileMode
}

type ConnectionStatus struct {
	Timestamp time.Time
	Connected bool
}

type Heartbeat struct {
	UID       int
	Timestamp time.Time
}

type Occupant struct {
	UID      int
	FileMode dfslib.FileMode
}

type FileData struct {
	FileName string
	FilePath string
	Chunks   []int
}

////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
///////////////////// HELPER METHODS ///////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////

func checkError(err error) bool {
	return (err != nil)
}

func containsUID(list []int, uid int) bool {
	for _, n := range list {
		if n == uid {
			return true
		}
	}
	return false
}

func containsFileName(files []string, fname string) bool {
	for _, n := range files {
		if n == fname {
			return true
		}
	}
	return false
}

func updateClientFileLedger(files []string, uid int) {
	for _, file := range files {
		if clientList, exists := fileToClientLedger[file]; exists {
			if !containsUID(clientList, uid) {
				clientList = append(clientList, uid)
				fileToClientLedger[file] = clientList
			}
		} else {
			newClientList := []int{uid}
			fileToClientLedger[file] = newClientList
		}
	}
}

func updateFileVersionLedger(files []string) {
	for _, file := range files {
		if _, exists := fileToVersionLedger[file]; !exists {
			fileToVersionLedger[file] = createVersionMap()
		}
	}
}

func updateConnectClientLedger(uid int, isConnected bool) {
	if _, exists := connectedClientLedger[uid]; exists {
		conn := connectedClientLedger[uid]
		conn.Connected = isConnected
		connectedClientLedger[uid] = conn
	} else {
		connectedClientLedger[uid] = ConnectionStatus{Timestamp: time.Now(), Connected: isConnected}
	}
}

func createVersionMap() map[int]([][]int) {
	chunkMap := map[int]([][]int){}

	for i := 0; i < 256; i++ {
		chunkMap[i] = [][]int{}
	}

	return chunkMap
}

func getDataFromClients(fname string, chunksByOnline map[int]([]int)) (map[int]([]byte), error) {
	chunkToData := make(map[int]([]byte))

	for uid, chunks := range chunksByOnline {
		clientInfo := clientLedger[uid]
		clientAddr := clientInfo.IP + ":" + clientInfo.Port

		server, _ := rpc.Dial("tcp", clientAddr)

		json, _ := json.Marshal(FileData{FileName: fname, FilePath: clientInfo.FilePath, Chunks: chunks})
		var response *map[int]([]byte)
		if server != nil {
			server.Call("ClientCalls.ReadFile", json, &response)
		}

		chunkToData = mergeChunkDataMaps(chunkToData, *response)
	}

	return chunkToData, nil
}

func getOnlineFileHolders(fname string) []int {
	onlineUsers := []int{}

	for _, uid := range fileToClientLedger[fname] {
		if connectedClientLedger[uid].Connected {
			onlineUsers = append(onlineUsers, uid)
		}
	}

	return onlineUsers
}

func getChunksByClients(fname string, onlineUsers []int) (map[int]([]int), map[int]([]int)) {
	clientsToChunks := map[int]([]int){}
	onlineToChunks := map[int]([]int){}
	for _, n := range onlineUsers {
		onlineToChunks[n] = []int{}
	}

	allChunks := fileToVersionLedger[fname]
	foundLatest := false

	for chunkNum, versionMatrix := range allChunks {

		for i := len(versionMatrix) - 1; i >= 0; i-- {
			clients := versionMatrix[i]

			for _, id := range clients {

				if fChunk, exists := clientsToChunks[id]; !exists {
					clientsToChunks[id] = []int{chunkNum}
				} else {
					fChunk = append(fChunk, chunkNum)
					clientsToChunks[id] = fChunk
				}

				if oChunk, online := onlineToChunks[id]; online {
					oChunk = append(oChunk, chunkNum)
					onlineToChunks[id] = oChunk
					foundLatest = true
					break
				}
			}

			if foundLatest {
				foundLatest = false
				break
			}
		}
	}

	return clientsToChunks, onlineToChunks
}

func mergeChunkDataMaps(result map[int]([]byte), data map[int]([]byte)) map[int]([]byte) {
	for k, v := range data {
		result[k] = v
	}

	return result
}

func findLatestClientWithChunk(fname string, chunkNum int) int {
	chunkToVersion := fileToVersionLedger[fname]
	versionMatrix := chunkToVersion[chunkNum]

	// trivial
	if len(versionMatrix) == 0 {
		return 0
	}

	latestVersion := versionMatrix[len(versionMatrix)-1]
	for _, uid := range latestVersion {
		if connectedClientLedger[uid].Connected {
			return uid
		}
	}

	return -1
}

func updateFileVersioning(uid int, fname string, chunkNum int, isRead bool) {
	chunkToVersion := fileToVersionLedger[fname]
	versionMatrix := chunkToVersion[chunkNum]

	if !isRead {
		versionMatrix = append(versionMatrix, []int{uid})
	} else if len(versionMatrix) > 0 {
		lastRow := len(versionMatrix) - 1
		clientList := versionMatrix[lastRow]
		clientList = append(clientList, uid)
		versionMatrix[lastRow] = clientList
	}

	chunkToVersion[chunkNum] = versionMatrix
	fileToVersionLedger[fname] = chunkToVersion
}

func receiveHeartbeats() {
	for {
		time.Sleep(2 * time.Second)
		for uid, conn := range connectedClientLedger {
			if conn.Connected {
				prevTime := conn.Timestamp
				currTime := time.Now()

				if currTime.Sub(prevTime).Seconds() > 2 {
					conn.Connected = false
					connectedClientLedger[uid] = conn
				}
			}
		}
	}
}

////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////// RPC METHODS /////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////

type ServerCalls int

func (server *ServerCalls) Connect(data []byte, uid *int) (err error) {
	var client ClientInfo
	json.Unmarshal(bytes.Trim(data, "\x00"), &client)

	if _, exists := clientLedger[client.UID]; !exists {
		*uid = UID
		client.UID = UID
		UID = UID + 1
	} else {
		*uid = 0
	}
	clientLedger[client.UID] = client

	updateClientFileLedger(client.Files, client.UID)
	updateFileVersionLedger(client.Files)
	updateConnectClientLedger(client.UID, true)

	return nil
}

func (server *ServerCalls) Disconnect(uid int, isConnected *bool) (err error) {
	if !connectedClientLedger[uid].Connected {
		*isConnected = false
		return nil
	}
	*isConnected = true

	filesOnHold := clientToFileLedger[uid]
	for _, file := range filesOnHold {
		if occ, exists := fileOccupancyLedger[file]; exists {
			if occ.UID == uid {
				delete(fileOccupancyLedger, file)
			}
		}
	}

	delete(clientToFileLedger, uid)

	updateConnectClientLedger(uid, false)

	return nil
}

func (server *ServerCalls) Open(data []byte, response *OpenResponse) (err error) {
	var openRequest OpenRequest
	json.Unmarshal(bytes.Trim(data, "\x00"), &openRequest)

	uid := openRequest.UID
	fname := openRequest.FileName
	mode := openRequest.FileMode
	writeMode := dfslib.WRITE

	// OpenWriteConflictError
	if occ, exists := fileOccupancyLedger[fname]; exists {
		if occ.FileMode == writeMode && mode == writeMode && occ.UID != uid {
			*response = OpenResponse{OpenWriteConflictError: true}
			return nil
		}
	}

	if mode == writeMode {
		fileOccupancyLedger[fname] = Occupant{UID: uid, FileMode: mode}
	}

	// FileUnavailableError
	onlineFileHolders := getOnlineFileHolders(fname)
	chunksByClients, chunksByOnline := getChunksByClients(fname, onlineFileHolders)

	// You're the only one online, no chunks are the latest version (trivial case)
	if len(chunksByClients) == 0 {
		// Trivial Case!
	}
	// No one is online and chunks needed are non-trivial
	if len(onlineFileHolders) == 0 && len(chunksByClients) > 0 {
		*response = OpenResponse{FileUnavailableError: true}
		return nil
	}

	chunkToData, _ := getDataFromClients(fname, chunksByOnline)

	*response = OpenResponse{
		OpenWriteConflictError: false,
		FileUnavailableError:   false,
		Data:                   chunkToData,
	}

	// refactor
	if _, exists := clientToFileLedger[uid]; exists {
		files := clientToFileLedger[uid]

		if !containsFileName(files, fname) {
			files = append(files, fname)
			clientToFileLedger[uid] = files
		}
	} else {
		files := []string{fname}
		clientToFileLedger[uid] = files
	}

	// refactor
	if _, exists := fileToClientLedger[fname]; exists {
		clientList := fileToClientLedger[fname]

		if !containsUID(clientList, uid) {
			clientList = append(clientList, uid)
			fileToClientLedger[fname] = clientList
		}
	} else {
		clientList := []int{uid}
		fileToClientLedger[fname] = clientList
	}

	return nil
}

func (server *ServerCalls) GlobalFileCheck(data []byte, response *GlobalSearchResponse) (err error) {
	var globalSearchRequest GlobalSearchRequest
	json.Unmarshal(bytes.Trim(data, "\x00"), &globalSearchRequest)

	fname := globalSearchRequest.FileName

	_, exists := fileToClientLedger[fname]
	*response = GlobalSearchResponse{FileExists: exists}

	return nil
}

func (server *ServerCalls) UpdateFiles(data []byte, isConnected *bool) (err error) {
	var updateRequest UpdateRequest
	json.Unmarshal(bytes.Trim(data, "\x00"), &updateRequest)

	uid := updateRequest.UID
	fname := updateRequest.FileName
	mode := updateRequest.FileMode

	// DisconnectError
	if !connectedClientLedger[uid].Connected {
		*isConnected = false
		return nil
	}
	*isConnected = true

	// Add file to client info
	client := clientLedger[uid]
	client.Files = append(client.Files, fname)

	// Create file version matrix for new file
	if _, exists := fileToVersionLedger[fname]; !exists {
		fileToVersionLedger[fname] = createVersionMap()
	}

	// Occupy file if mode is WRITE (REFACTOR)
	if mode == dfslib.WRITE {
		fileOccupancyLedger[fname] = Occupant{UID: uid, FileMode: mode}
	}

	// refactor
	if _, exists := clientToFileLedger[uid]; exists {
		files := clientToFileLedger[uid]

		if !containsFileName(files, fname) {
			files = append(files, fname)
			clientToFileLedger[uid] = files
		}
	} else {
		files := []string{fname}
		clientToFileLedger[uid] = files
	}

	// refactor
	if _, exists := fileToClientLedger[fname]; exists {
		clientList := fileToClientLedger[fname]

		if !containsUID(clientList, uid) {
			clientList = append(clientList, uid)
			fileToClientLedger[fname] = clientList
		}
	} else {
		clientList := []int{uid}
		fileToClientLedger[fname] = clientList
	}

	return nil
}

func (server *ServerCalls) Read(data []byte, response *ReadResponse) (err error) {
	var readRequest ReadRequest
	json.Unmarshal(bytes.Trim(data, "\x00"), &readRequest)

	uid := readRequest.UID
	fname := readRequest.FileName
	chunkNum := readRequest.ChunkNum

	if !connectedClientLedger[uid].Connected {
		*response = ReadResponse{DisconnectedError: true}
		return nil
	}

	clientToCallUID := findLatestClientWithChunk(fname, chunkNum)
	// trivial
	if clientToCallUID == 0 {
		clientToCallUID = uid
	}
	if clientToCallUID < 0 {
		*response = ReadResponse{ChunkUnavailableError: true}
		return nil
	}

	clientToCall := clientLedger[clientToCallUID]
	clientToCallAddr := clientToCall.IP + ":" + clientToCall.Port

	caller, dialErr := rpc.Dial("tcp", clientToCallAddr)
	if checkError(dialErr) {
		return dialErr
	}

	var reply dfslib.Chunk
	requestToClient := ReadRequest{
		UID:      clientToCall.UID,
		ChunkNum: chunkNum,
		FileName: fname,
		FilePath: clientToCall.FilePath,
	}
	json, _ := json.Marshal(requestToClient)
	if caller != nil {
		callErr := caller.Call("ClientCalls.ReadChunk", json, &reply)
		if checkError(callErr) {
			return callErr
		}
	}

	*response = ReadResponse{
		ChunkUnavailableError: false,
		DisconnectedError:     false,
		Data:                  reply,
	}

	// update versioning for chunk
	updateFileVersioning(uid, fname, chunkNum, true)

	return nil
}

func (server *ServerCalls) ValidateWrite(uid int, isConnected *bool) (err error) {
	*isConnected = connectedClientLedger[uid].Connected
	return nil
}

func (server *ServerCalls) Write(data []byte, isSuccessful *bool) (err error) {
	var writeRequest WriteRequest
	json.Unmarshal(bytes.Trim(data, "\x00"), &writeRequest)

	uid := writeRequest.UID
	fname := writeRequest.FileName
	chunkNum := writeRequest.ChunkNum

	updateFileVersioning(uid, fname, chunkNum, false)

	*isSuccessful = true
	return nil
}

func (server *ServerCalls) Close(data []byte, isConnected *bool) (err error) {
	var closeRequest CloseRequest
	json.Unmarshal(bytes.Trim(data, "\x00"), &closeRequest)

	uid := closeRequest.UID
	fname := closeRequest.FileName

	if !connectedClientLedger[uid].Connected {
		*isConnected = false
		return nil
	}

	filesOnHold := clientToFileLedger[uid]

	for _, file := range filesOnHold {
		if fname == file {
			if occ, exists := fileOccupancyLedger[fname]; exists {
				if occ.UID == uid && occ.FileMode == dfslib.WRITE {
					delete(fileOccupancyLedger, fname)
					break
				}
			}
		}
	}

	*isConnected = true
	return nil
}

func (server *ServerCalls) PingServer(data []byte, isConnected *bool) (err error) {
	var heartbeat Heartbeat
	json.Unmarshal(bytes.Trim(data, "\x00"), &heartbeat)

	uid := heartbeat.UID
	time := heartbeat.Timestamp

	conn, exists := connectedClientLedger[uid]

	if exists && conn.Connected {
		conn := connectedClientLedger[uid]
		conn.Timestamp = time
		connectedClientLedger[uid] = conn
	}

	*isConnected = conn.Connected
	return nil
}

////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////// MAIN ////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////

func main() {
	serverAddr, resolveErr := net.ResolveTCPAddr("tcp", os.Args[1])
	if checkError(resolveErr) {
		return
	}

	inbound, inboundErr := net.ListenTCP("tcp", serverAddr)
	if checkError(inboundErr) {
		return
	}

	serverCalls := new(ServerCalls)
	rpc.Register(serverCalls)

	go receiveHeartbeats()

	for {
		conn, err := inbound.Accept()
		if checkError(err) {
			continue
		}

		time.Sleep(200 * time.Millisecond)
		go rpc.ServeConn(conn)
	}
}
