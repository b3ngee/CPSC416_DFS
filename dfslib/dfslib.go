/*

This package specifies the application's interface to the distributed
file system (DFS) system to be used in assignment 2 of UBC CS 416
2017W2.

*/

package dfslib

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Server Address
var serverAddress string

// A Chunk is the unit of reading/writing in DFS.
type Chunk [32]byte

// Represents a type of file access.
type FileMode int

const (
	// Read mode.
	READ FileMode = iota

	// Read/Write mode.
	WRITE

	// Disconnected read mode.
	DREAD
)

////////////////////////////////////////////////////////////////////////////////////////////
// <ERROR DEFINITIONS>

// These type definitions allow the application to explicitly check
// for the kind of error that occurred. Each API call below lists the
// errors that it is allowed to raise.
//
// Also see:
// https://blog.golang.org/error-handling-and-go
// https://blog.golang.org/errors-are-values

// Contains serverAddr
type DisconnectedError string

func (e DisconnectedError) Error() string {
	return fmt.Sprintf("DFS: Not connnected to server [%s]", string(e))
}

// Contains chunkNum that is unavailable
type ChunkUnavailableError uint8

func (e ChunkUnavailableError) Error() string {
	return fmt.Sprintf("DFS: Latest verson of chunk [%d] unavailable", e)
}

// Contains filename
type OpenWriteConflictError string

func (e OpenWriteConflictError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] is opened for writing by another client", string(e))
}

// Contains filename.
type WriteModeTimeoutError string

func (e WriteModeTimeoutError) Error() string {
	return fmt.Sprintf("DFS: Write access to filename [%s] has timed out; reopen the file", string(e))
}

// Contains file mode that is bad.
type BadFileModeError FileMode

func (e BadFileModeError) Error() string {
	return fmt.Sprintf("DFS: Cannot perform this operation in current file mode [%s]", string(e))
}

// Contains filename
type BadFilenameError string

func (e BadFilenameError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] includes illegal characters or has the wrong length", string(e))
}

// Contains filename
type FileUnavailableError string

func (e FileUnavailableError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] is unavailable", string(e))
}

// Contains local path
type LocalPathError string

func (e LocalPathError) Error() string {
	return fmt.Sprintf("DFS: Cannot access local path [%s]", string(e))
}

// Contains filename
type FileDoesNotExistError string

func (e FileDoesNotExistError) Error() string {
	return fmt.Sprintf("DFS: Cannot open file [%s] in D mode as it does not exist locally", string(e))
}

////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
//////////////////////// STRUCTS ///////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////

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
	FileMode FileMode
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
	Data                  Chunk
}

type WriteRequest struct {
	UID      int
	ChunkNum int
	FileName string
}

type UpdateRequest struct {
	UID      int
	FileName string
	FileMode FileMode
}

type FileData struct {
	FileName string
	FilePath string
	Chunks   []int
}

type Heartbeat struct {
	UID       int
	Timestamp time.Time
}

// </ERROR DEFINITIONS>
////////////////////////////////////////////////////////////////////////////////////////////

// Represents a file in the DFS system.
type DFSFile interface {
	// Reads chunk number chunkNum into storage pointed to by
	// chunk. Returns a non-nil error if the read was unsuccessful.
	//
	// Can return the following errors:
	// - DisconnectedError (in READ,WRITE modes)
	// - ChunkUnavailableError (in READ,WRITE modes)
	Read(chunkNum uint8, chunk *Chunk) (err error)

	// Writes chunk number chunkNum from storage pointed to by
	// chunk. Returns a non-nil error if the write was unsuccessful.
	//
	// Can return the following errors:
	// - BadFileModeError (in READ,DREAD modes)
	// - DisconnectedError (in WRITE mode)
	Write(chunkNum uint8, chunk *Chunk) (err error)

	// Closes the file/cleans up. Can return the following errors:
	// - DisconnectedError
	Close() (err error)
}

// Represents a connection to the DFS system.
type DFS interface {
	// Check if a file with filename fname exists locally (i.e.,
	// available for DREAD reads).
	//
	// Can return the following errors:
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	LocalFileExists(fname string) (exists bool, err error)

	// Check if a file with filename fname exists globally.
	//
	// Can return the following errors:
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	// - DisconnectedError
	GlobalFileExists(fname string) (exists bool, err error)

	// Opens a filename with name fname using mode. Creates the file
	// in READ/WRITE modes if it does not exist. Returns a handle to
	// the file through which other operations on this file can be
	// made.
	//
	// Can return the following errors:
	// - OpenWriteConflictError (in WRITE mode)
	// - DisconnectedError (in READ,WRITE modes)
	// - FileUnavailableError (in READ,WRITE modes)
	// - FileDoesNotExistError (in DREAD mode)
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	Open(fname string, mode FileMode) (f DFSFile, err error)

	// Disconnects from the server. Can return the following errors:
	// - DisconnectedError
	UMountDFS() (err error)
}

type DFSStruct struct {
	Client     *rpc.Client
	ClientInfo ClientInfo
	Connected  *bool
}

func (dfs DFSStruct) LocalFileExists(fname string) (exists bool, err error) {
	// Check if a file with filename fname exists locally (i.e. available for DREAD reads).
	//
	// Can return the following errors:
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)

	if !isFileNameValid(fname) {
		return false, BadFilenameError(fname)
	}

	// Check if file exists in file path
	localPath := dfs.ClientInfo.FilePath
	fullPath := constructFullPath(localPath, fname)

	_, err = os.Stat(fullPath)

	return !os.IsNotExist(err), nil
}

func (dfs DFSStruct) GlobalFileExists(fname string) (exists bool, err error) {
	// Check if a file with filename fname exists globally.
	//
	// Can return the following errors:
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	// - DisconnectedError

	if !*dfs.Connected {
		return false, DisconnectedError(serverAddress)
	}

	if !isFileNameValid(fname) {
		return false, BadFilenameError(fname)
	}

	client := dfs.Client
	clientInfo := dfs.ClientInfo
	uid := clientInfo.UID

	globalSearchRequest := GlobalSearchRequest{UID: uid, FileName: sanitizeFileName(fname)}

	var response GlobalSearchResponse
	json, _ := json.Marshal(globalSearchRequest)
	if client != nil {
		client.Call("ServerCalls.GlobalFileCheck", json, &response)
	}

	return response.FileExists, nil
}

func (dfs DFSStruct) Open(fname string, mode FileMode) (f DFSFile, err error) {
	// Opens a filename with name fname using mode. Creates the file
	// in READ/WRITE modes if it does not exist. Returns a handle to
	// the file through which other operations on this file can be
	// made.
	//
	// Can return the following errors:
	// - OpenWriteConflictError (in WRITE mode)
	// - DisconnectedError (in READ,WRITE modes)
	// - FileUnavailableError (in READ,WRITE modes)
	// - FileDoesNotExistError (in DREAD mode)
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	client := dfs.Client
	clientInfo := dfs.ClientInfo

	// DisconnectedError (in READ, WRITE modes)
	if !*dfs.Connected && (mode == READ || mode == WRITE) {
		return nil, DisconnectedError(serverAddress)
	}

	// BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	if !isFileNameValid(fname) {
		return nil, BadFilenameError(fname)
	}

	// FileDoesNotExistError (in DREAD mode)
	if mode == DREAD && !*dfs.Connected {
		if dreadExist, _ := dfs.LocalFileExists(fname); !dreadExist {
			return nil, FileDoesNotExistError(fname)
		}
		dfsFile := DFSFileStruct{
			Client:      client,
			UID:         clientInfo.UID,
			IP:          clientInfo.IP,
			Port:        clientInfo.Port,
			FileName:    sanitizeFileName(fname),
			FilePath:    clientInfo.FilePath,
			FileMode:    mode,
			IsConnected: false,
		}

		return dfsFile, nil
	}

	// File doesn't exist Globally, create locally and broadcast to server
	globalExists, globalExistErr := dfs.GlobalFileExists(fname)
	if checkError(globalExistErr) {
		return nil, globalExistErr
	}

	if !globalExists {
		fullPath := constructFullPath(clientInfo.FilePath, fname)
		createTrivialFile(fullPath)

		updateRequest := UpdateRequest{
			UID:      clientInfo.UID,
			FileName: sanitizeFileName(fname),
			FileMode: mode,
		}

		json, _ := json.Marshal(updateRequest)
		var ack bool
		if client != nil {
			updateErr := client.Call("ServerCalls.UpdateFiles", json, &ack)
			if checkError(updateErr) {
				return nil, DisconnectedError(serverAddress)
			}
		}

		dfsFile := DFSFileStruct{
			Client:      client,
			UID:         clientInfo.UID,
			IP:          clientInfo.IP,
			Port:        clientInfo.Port,
			FileName:    sanitizeFileName(fname),
			FilePath:    clientInfo.FilePath,
			FileMode:    mode,
			IsConnected: true,
		}

		return dfsFile, nil
	}

	localFileExists, _ := dfs.LocalFileExists(fname)
	fullPath := constructFullPath(clientInfo.FilePath, fname)
	if !localFileExists {
		createTrivialFile(fullPath)
	}

	json, _ := json.Marshal(OpenRequest{
		UID:      clientInfo.UID,
		IP:       clientInfo.IP,
		Port:     clientInfo.Port,
		FileName: sanitizeFileName(fname),
		FilePath: clientInfo.FilePath,
		FileMode: mode,
	})

	var response OpenResponse
	if client != nil {
		openErr := client.Call("ServerCalls.Open", json, &response)
		if checkError(openErr) {
			return nil, DisconnectedError(serverAddress)
		}
	}
	if response.OpenWriteConflictError {
		return nil, OpenWriteConflictError(fname)
	}
	if response.FileUnavailableError {
		return nil, FileUnavailableError(fname)
	}

	writeFileByChunks(fullPath, response.Data)

	dfsFile := DFSFileStruct{
		Client:      client,
		UID:         clientInfo.UID,
		IP:          clientInfo.IP,
		Port:        clientInfo.Port,
		FileName:    sanitizeFileName(fname),
		FilePath:    clientInfo.FilePath,
		FileMode:    mode,
		IsConnected: true,
	}

	return dfsFile, nil
}

func (dfs DFSStruct) UMountDFS() (err error) {
	// Disconnects from the server. Can return the following errors:
	// - DisconnectedError

	if !*dfs.Connected {
		return DisconnectedError(serverAddress)
	}

	client := dfs.Client
	clientInfo := dfs.ClientInfo
	uid := clientInfo.UID

	var ack bool

	if client != nil {
		disconnectErr := client.Call("ServerCalls.Disconnect", uid, &ack)
		if checkError(disconnectErr) {
			return DisconnectedError(serverAddress)
		}
		client.Close()
	}

	return nil
}

func (dfs DFSStruct) sendHeartbeat(client *rpc.Client, uid int) {
	for {
		time.Sleep(2 * time.Second)

		heartbeat := Heartbeat{UID: uid, Timestamp: time.Now()}
		json, _ := json.Marshal(heartbeat)

		var isConnected bool
		if client != nil {
			client.Call("ServerCalls.PingServer", json, &isConnected)
		}

		*dfs.Connected = isConnected

		if !isConnected {
			return
		}
	}
}

type DFSFileStruct struct {
	Client      *rpc.Client
	UID         int
	IP          string
	Port        string
	FileName    string
	FilePath    string
	FileMode    FileMode
	IsConnected bool
}

func (dfsf DFSFileStruct) Read(chunkNum uint8, chunk *Chunk) (err error) {
	// Reads chunk number chunkNum into storage pointed to by
	// chunk. Returns a non-nil error if the read was unsuccessful.
	//
	// Can return the following errors:
	// - DisconnectedError (in READ,WRITE modes)
	// - ChunkUnavailableError (in READ,WRITE modes)
	client := dfsf.Client
	fname := dfsf.FileName
	path := dfsf.FilePath

	if !dfsf.IsConnected && dfsf.FileMode == DREAD {
		data, _ := readSingleChunk(constructFullPath(path, fname), int(chunkNum))
		*chunk = data
		return nil
	}

	readRequest := ReadRequest{
		UID:      dfsf.UID,
		ChunkNum: int(chunkNum),
		FileName: fname,
		FilePath: path,
	}
	json, _ := json.Marshal(readRequest)
	var reply ReadResponse
	if client != nil {
		callErr := client.Call("ServerCalls.Read", json, &reply)
		if checkError(callErr) {
			return DisconnectedError(serverAddress)
		}
	}

	if reply.ChunkUnavailableError {
		return ChunkUnavailableError(chunkNum)
	}
	if reply.DisconnectedError {
		return DisconnectedError(serverAddress)
	}

	*chunk = reply.Data
	writeSingleChunk(constructFullPath(path, fname), int(chunkNum), *chunk)

	return nil
}

func (dfsf DFSFileStruct) Write(chunkNum uint8, chunk *Chunk) (err error) {
	// Writes chunk number chunkNum from storage pointed to by
	// chunk. Returns a non-nil error if the write was unsuccessful.
	//
	// Can return the following errors:
	// - BadFileModeError (in READ,DREAD modes)
	// - DisconnectedError (in WRITE mode)

	client := dfsf.Client
	uid := dfsf.UID
	fname := dfsf.FileName
	path := dfsf.FilePath
	mode := dfsf.FileMode

	if mode != WRITE {
		return BadFileModeError(mode)
	}

	var isConnected bool
	if client != nil {
		validateErr := client.Call("ServerCalls.ValidateWrite", uid, &isConnected)
		if checkError(validateErr) || !isConnected {
			return DisconnectedError(serverAddress)
		}
	}

	writeSingleChunk(constructFullPath(path, fname), int(chunkNum), *chunk)

	writeRequest := WriteRequest{
		UID:      dfsf.UID,
		ChunkNum: int(chunkNum),
		FileName: dfsf.FileName,
	}
	json, _ := json.Marshal(writeRequest)
	var isSuccessful bool
	if client != nil {
		callErr := client.Call("ServerCalls.Write", json, &isSuccessful)
		if checkError(callErr) || !isSuccessful {
			return DisconnectedError(serverAddress)
		}
	}

	return nil
}

func (dfsf DFSFileStruct) Close() (err error) {
	// Closes the file/cleans up. Can return the following errors:
	// - DisconnectedError

	client := dfsf.Client
	uid := dfsf.UID
	fname := dfsf.FileName

	closeRequest := CloseRequest{UID: uid, FileName: fname}
	json, _ := json.Marshal(closeRequest)
	var isConnected bool
	if client != nil {
		callErr := client.Call("ServerCalls.Close", json, &isConnected)
		if checkError(callErr) || !isConnected {
			return DisconnectedError(serverAddress)
		}
	}

	return nil
}

////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////// RPC METHODS /////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////

type ClientCalls int

func (client *ClientCalls) ReadFile(data []byte, reply *map[int]([]byte)) (err error) {
	var fileData FileData
	json.Unmarshal(bytes.Trim(data, "\x00"), &fileData)

	fullPath := constructFullPath(fileData.FilePath, fileData.FileName)
	readData, _ := readFileByChunks(fullPath, fileData.Chunks)

	*reply = readData

	return nil
}

func (client *ClientCalls) ReadChunk(data []byte, reply *Chunk) (err error) {
	var readRequest ReadRequest
	json.Unmarshal(bytes.Trim(data, "\x00"), &readRequest)

	fname := readRequest.FileName
	chunkNum := readRequest.ChunkNum
	path := readRequest.FilePath

	result, _ := readSingleChunk(constructFullPath(path, fname), chunkNum)

	*reply = result

	return nil
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

func fetchClientUID(fullPath string) (int, error) {
	if _, err := os.Stat(fullPath); !os.IsNotExist(err) {
		text, readErr := readFromFile(fullPath)
		if checkError(readErr) {
			return -1, readErr
		}

		uid, parseErr := strconv.ParseInt(string(bytes.Trim(text, "\x00")), 10, 32)
		if checkError(parseErr) {
			return -1, parseErr
		}

		return int(uid), nil
	}

	return 0, nil
}

func getAllLocalFiles(path string) ([]string, error) {
	var clientFiles []string

	allFiles, readDirErr := ioutil.ReadDir(santizePath(path))
	if checkError(readDirErr) {
		return nil, readDirErr
	}

	for _, file := range allFiles {
		clientFiles = append(clientFiles, file.Name())
	}

	return clientFiles, nil
}

func constructFullPath(path string, fname string) string {
	return santizePath(path) + sanitizeFileName(fname)
}

func santizePath(path string) string {
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}
	return path
}

func sanitizeFileName(fname string) string {
	if !strings.HasSuffix(fname, ".dfs") {
		fname = fname + ".dfs"
	}
	return fname
}

func sanitizeIP(localIP string) (string, string) {
	if strings.Contains(localIP, ":") {
		splitIP := strings.Split(localIP, ":")
		return splitIP[0], splitIP[1]
	}

	return localIP, "0"
}

func isFileNameValid(fname string) bool {
	isNumberOrLetters := regexp.MustCompile(`^[a-z0-9]+$`).MatchString
	return isNumberOrLetters(fname) && len(fname) <= 16 && len(fname) > 0
}

func createFileOnLocalPath(path string) (*os.File, error) {
	newFile, err := os.Create(path)
	if checkError(err) {
		return nil, err
	}
	return newFile, nil
}

func createTrivialFile(path string) error {
	newFile, createErr := createFileOnLocalPath(path)
	if checkError(createErr) {
		return createErr
	}

	trivial := make([]byte, 8192)
	_, writeErr := newFile.Write(trivial)
	if checkError(writeErr) {
		return writeErr
	}

	newFile.Close()
	return nil
}

func writeToFile(path string, message string) error {
	file, openErr := os.OpenFile(path, os.O_RDWR, 0644)
	if checkError(openErr) {
		return openErr
	}

	defer file.Close()

	_, writeErr := file.WriteString(message)
	if checkError(writeErr) {
		return writeErr
	}

	// Save changes
	syncErr := file.Sync()
	if checkError(syncErr) {
		return syncErr
	}

	return nil
}

func readFromFile(path string) ([]byte, error) {
	file, openErr := os.OpenFile(path, os.O_RDONLY, 0644)
	if checkError(openErr) {
		return nil, openErr
	}

	defer file.Close()

	var text = make([]byte, 1024)
	for {
		_, readErr := file.Read(text)

		// break if finally arrived at end of file
		if readErr == io.EOF {
			break
		}

		// break if error occured
		if readErr != nil && readErr != io.EOF {
			return nil, readErr
		}
	}

	return text, nil
}

func readFileByChunks(path string, chunks []int) (map[int]([]byte), error) {
	result := map[int]([]byte){}

	file, openErr := os.OpenFile(path, os.O_RDONLY, 0644)
	if checkError(openErr) {
		return nil, openErr
	}

	defer file.Close()

	for _, chunk := range chunks {
		chunkSlice := make([]byte, 32)

		_, readErr := file.ReadAt(chunkSlice, int64(32*chunk))
		if checkError(readErr) {
			return nil, readErr
		}
		result[chunk] = chunkSlice
	}

	return result, nil
}

func readSingleChunk(path string, chunkNum int) (Chunk, error) {
	var result Chunk

	file, openErr := os.OpenFile(path, os.O_RDONLY, 0644)
	if checkError(openErr) {
		return result, openErr
	}

	defer file.Close()

	_, readErr := file.ReadAt(result[:], int64(32*chunkNum))
	if checkError(readErr) {
		return result, readErr
	}

	return result, nil
}

func writeFileByChunks(path string, chunks map[int]([]byte)) error {
	file, openErr := os.OpenFile(path, os.O_RDWR, 0644)
	if checkError(openErr) {
		return openErr
	}

	defer file.Close()

	for k, v := range chunks {
		_, writeErr := file.WriteAt(v, int64(32*k))
		if checkError(writeErr) {
			return writeErr
		}
	}

	return nil
}

func writeSingleChunk(path string, chunkNum int, data Chunk) error {
	file, openErr := os.OpenFile(path, os.O_RDWR, 0644)
	if checkError(openErr) {
		return openErr
	}

	defer file.Close()

	_, writeErr := file.WriteAt(data[:], int64(32*chunkNum))
	if checkError(writeErr) {
		return writeErr
	}

	return nil
}

func listenForServer(localIP string, inChan chan string) error {
	clientAddr, resolveErr := net.ResolveTCPAddr("tcp", localIP)
	if checkError(resolveErr) {
		return resolveErr
	}

	inbound, inboundErr := net.ListenTCP("tcp", clientAddr)
	inChan <- inbound.Addr().String()
	if checkError(inboundErr) {
		return inboundErr
	}

	clientCalls := new(ClientCalls)
	rpc.RegisterName("ClientCalls", clientCalls)

	for {
		conn, err := inbound.Accept()
		if checkError(err) {
			continue
		}

		go rpc.ServeConn(conn)
	}
}

// The constructor for a new DFS object instance. Takes the server's
// IP:port address string as parameter, the localIP to use to
// establish the connection to the server, and a localPath path on the
// local filesystem where the client has allocated storage (and
// possibly existing state) for this DFS.
//
// The returned dfs instance is singleton: an application is expected
// to interact with just one dfs at a time.
//
// This call should succeed regardless of whether the server is
// reachable. Otherwise, applications cannot access (local) files
// while disconnected.
//
// Can return the following errors:
// - LocalPathError
// - Networking errors related to localIP or serverAddr
func MountDFS(serverAddr string, localIP string, localPath string) (dfs DFS, err error) {
	serverAddress = serverAddr
	const clientUIDFile = "clientUID"

	fullPath := constructFullPath(localPath, clientUIDFile)
	ip, port := sanitizeIP(localIP)
	localIP = ip + ":" + port

	////////////////////////////////////////////////////////////

	client, _ := rpc.Dial("tcp", serverAddr)
	clientUID, _ := fetchClientUID(fullPath)

	////////////////////////////////////////////////////////////

	inChan := make(chan string)
	go listenForServer(localIP, inChan)
	clientIP := <-inChan

	////////////////////////////////////////////////////////////

	localFiles, localErr := getAllLocalFiles(localPath)
	if checkError(localErr) {
		return nil, LocalPathError(localPath)
	}

	ip, port = sanitizeIP(clientIP)
	clientInfo := ClientInfo{
		UID:      clientUID,
		IP:       ip,
		Port:     port,
		FilePath: localPath,
		Files:    localFiles,
	}

	var assignedUID int
	json, _ := json.Marshal(clientInfo)

	if client != nil {
		client.Call("ServerCalls.Connect", json, &assignedUID)
	}

	////////////////////////////////////////////////////////////

	if assignedUID > 0 {
		clientInfo.UID = assignedUID

		_, createFileErr := createFileOnLocalPath(fullPath)
		if checkError(createFileErr) {
			return nil, LocalPathError(localPath)
		}
		writeToFileErr := writeToFile(fullPath, strconv.FormatInt(int64(assignedUID), 10))
		if checkError(writeToFileErr) {
			return nil, LocalPathError(localPath)
		}
	}

	isConnected := client != nil
	dfsStruct := DFSStruct{
		Client:     client,
		ClientInfo: clientInfo,
		Connected:  &isConnected,
	}

	if isConnected {
		go dfsStruct.sendHeartbeat(client, clientInfo.UID)
	}

	return dfsStruct, nil
}
