package raft

import (
	"fmt"
	"log"
)

// Debugging
const Debug = false
// const Debug = false

type logTopic string
const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "Drop"
	dError   logTopic = "Error"
	dInfo    logTopic = "Info"
	dLeader  logTopic = "Lead"
	dEntry   logTopic = "Entry"
	dLog    logTopic = "Log"
	dPersist logTopic = "Pers"
	dSnap    logTopic = "Snap"
	dTerm    logTopic = "Term"
	dTest    logTopic = "Test"
	dTimer   logTopic = "Timer"
	dTrace   logTopic = "Trac"
	dVote    logTopic = "Vote"
	dWarn    logTopic = "Warn"
)
var colorBegin = []string{
	"\033[44m",			// 蓝 
	"\033[41m", 		// 红
	"\033[43m",			// 黄
	"\033[45m",			// 紫
	"\033[46m",			// 浅蓝
	"\033[47m",			// 灰
	"\033[40m",			// 黑
}
var colorEnd = "\033[0m"

func DPrintf(server int, topic logTopic, format string, a ...interface{}) {
	if Debug {
		str := fmt.Sprintf("[server %d][%s]", server, topic)
		format = str + format
		format = colorBegin[server] + format + colorEnd
		log.Printf(format, a...)
	}
}
