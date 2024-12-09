// Copyright 2020 thinkgos (thinkgo@aliyun.com).  All rights reserved.
// Use of this source code is governed by a version 3 of the GNU General
// Public License, license that can be found in the LICENSE file.

package cs104

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agile-edge/go-iecp5/asdu"
	"github.com/agile-edge/go-mod-core-contracts/v3/clients/logger"
)

const (
	inactive = iota
	active
)

// Client is an IEC104 master
type Client struct {
	option  ClientOption
	conn    net.Conn
	handler ClientHandlerInterface

	// channel
	rcvASDU  chan []byte // for received asdu
	sendASDU chan []byte // for send asdu
	rcvRaw   chan []byte // for recvLoop raw cs104 frame
	sendRaw  chan []byte // for sendLoop raw cs104 frame

	// I帧的发送与接收序号
	seqNoSend uint16 // sequence number of next outbound I-frame
	ackNoSend uint16 // outbound sequence number yet to be confirmed
	seqNoRcv  uint16 // sequence number of next inbound I-frame
	ackNoRcv  uint16 // inbound sequence number yet to be confirmed

	// maps sendTime I-frames to their respective sequence number
	pending []seqPending

	startDtActiveSendSince atomic.Value // 当发送startDtActive时,等待确认回复的超时间隔
	stopDtActiveSendSince  atomic.Value // 当发起stopDtActive时,等待确认回复的超时

	// 连接状态
	status   uint32
	rwMux    sync.RWMutex
	isActive uint32

	// 其他
	logger logger.LoggingClient

	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	closeCancel context.CancelFunc

	onConnect        func(c *Client)
	onConnectionLost func(c *Client)
}

// NewClient returns an IEC104 master,default config and default asdu.ParamsWide params
func NewClient(handler ClientHandlerInterface, o *ClientOption, lc logger.LoggingClient) *Client {
	return &Client{
		option:           *o,
		handler:          handler,
		rcvASDU:          make(chan []byte, o.config.RecvUnAckLimitW<<4),
		sendASDU:         make(chan []byte, o.config.SendUnAckLimitK<<4),
		rcvRaw:           make(chan []byte, o.config.RecvUnAckLimitW<<5),
		sendRaw:          make(chan []byte, o.config.SendUnAckLimitK<<5), // may not block!
		logger:           lc,
		onConnect:        func(*Client) {},
		onConnectionLost: func(*Client) {},
	}
}

// SetOnConnectHandler set on connect handler
func (sf *Client) SetOnConnectHandler(f func(c *Client)) *Client {
	if f != nil {
		sf.onConnect = f
	}
	return sf
}

// SetConnectionLostHandler set connection lost handler
func (sf *Client) SetConnectionLostHandler(f func(c *Client)) *Client {
	if f != nil {
		sf.onConnectionLost = f
	}
	return sf
}

// Start the server,and return quickly,if it nil,the server will disconnected background,other failed
func (sf *Client) Start() error {
	if sf.option.server == nil {
		return errors.New("empty remote server")
	}

	go sf.running()
	return nil
}

// Connect is
func (sf *Client) running() {
	lc := sf.logger
	var ctx context.Context

	sf.rwMux.Lock()
	if !atomic.CompareAndSwapUint32(&sf.status, initial, disconnected) {
		sf.rwMux.Unlock()
		return
	}
	ctx, sf.closeCancel = context.WithCancel(context.Background())
	sf.rwMux.Unlock()
	defer sf.setConnectStatus(initial)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		lc.Debugf("connecting server %+v", sf.option.server)
		conn, err := openConnection(sf.option.server, sf.option.TLSConfig, sf.option.config.ConnectTimeout0)
		if err != nil {
			lc.Errorf("connect failed, %v", err)
			if !sf.option.autoReconnect {
				return
			}
			time.Sleep(sf.option.reconnectInterval)
			continue
		}
		lc.Debugf("connect success")
		sf.conn = conn
		sf.run(ctx)

		lc.Debugf("disconnected server %+v", sf.option.server)
		select {
		case <-ctx.Done():
			return
		default:
			// 随机500ms-1s的重试，避免快速重试造成服务器许多无效连接
			time.Sleep(time.Millisecond * time.Duration(500+rand.Intn(500)))
		}
	}
}

func (sf *Client) recvLoop() {
	lc := sf.logger
	lc.Debugf("recvLoop started")
	defer func() {
		sf.cancel()
		sf.wg.Done()
		lc.Debugf("recvLoop stopped")
	}()

	for {
		rawData := make([]byte, APDUSizeMax)
		for rdCnt, length := 0, 2; rdCnt < length; {
			byteCount, err := io.ReadFull(sf.conn, rawData[rdCnt:length])
			if err != nil {
				// See: https://github.com/golang/go/issues/4373
				if err != io.EOF && err != io.ErrClosedPipe ||
					strings.Contains(err.Error(), "use of closed network connection") {
					lc.Errorf("receive failed, %v", err)
					return
				}
				if e, ok := err.(net.Error); ok && !e.Temporary() {
					lc.Errorf("receive failed, %v", err)
					return
				}
				if rdCnt == 0 && err == io.EOF {
					lc.Errorf("remote connect closed, %v", err)
					return
				}
			}

			rdCnt += byteCount
			if rdCnt == 0 {
				continue
			} else if rdCnt == 1 {
				if rawData[0] != startFrame {
					rdCnt = 0
					continue
				}
			} else {
				if rawData[0] != startFrame {
					rdCnt, length = 0, 2
					continue
				}
				length = int(rawData[1]) + 2
				if length < APCICtlFiledSize+2 || length > APDUSizeMax {
					rdCnt, length = 0, 2
					continue
				}
				if rdCnt == length {
					apdu := rawData[:length]
					lc.Debugf("RX Raw[% x]", apdu)
					sf.rcvRaw <- apdu
				}
			}
		}
	}
}

func (sf *Client) sendLoop() {
	lc := sf.logger
	lc.Debugf("sendLoop started")
	defer func() {
		sf.cancel()
		sf.wg.Done()
		lc.Debugf("sendLoop stopped")
	}()
	for {
		select {
		case <-sf.ctx.Done():
			return
		case apdu := <-sf.sendRaw:
			lc.Debugf("TX Raw[% x]", apdu)
			for wrCnt := 0; len(apdu) > wrCnt; {
				byteCount, err := sf.conn.Write(apdu[wrCnt:])
				if err != nil {
					// See: https://github.com/golang/go/issues/4373
					if err != io.EOF && err != io.ErrClosedPipe ||
						strings.Contains(err.Error(), "use of closed network connection") {
						lc.Errorf("sendRaw failed, %v", err)
						return
					}
					if e, ok := err.(net.Error); !ok || !e.Temporary() {
						lc.Errorf("sendRaw failed, %v", err)
						return
					}
					// temporary error may be recoverable
				}
				wrCnt += byteCount
			}
		}
	}
}

// run is the big fat state machine.
func (sf *Client) run(ctx context.Context) {
	lc := sf.logger
	lc.Debugf("run started!")
	// before any thing make sure init
	sf.cleanUp()

	sf.ctx, sf.cancel = context.WithCancel(ctx)
	sf.setConnectStatus(connected)
	sf.wg.Add(3)
	go sf.recvLoop()
	go sf.sendLoop()
	go sf.handlerLoop()

	var checkTicker = time.NewTicker(timeoutResolution)

	// transmission timestamps for timeout calculation
	var willNotTimeout = time.Now().Add(time.Hour * 24 * 365 * 100)

	var unAckRcvSince = willNotTimeout
	var idleTimeout3Sine = time.Now()         // 空闲间隔发起testFrAlive
	var testFrAliveSendSince = willNotTimeout // 当发起testFrAlive时,等待确认回复的超时间隔

	sf.startDtActiveSendSince.Store(willNotTimeout)
	sf.stopDtActiveSendSince.Store(willNotTimeout)

	sendSFrame := func(rcvSN uint16) {
		lc.Debugf("TX sFrame %v", sAPCI{rcvSN})
		sf.sendRaw <- newSFrame(rcvSN)
	}

	sendIFrame := func(asdu1 []byte) {
		seqNo := sf.seqNoSend

		iframe, err := newIFrame(seqNo, sf.seqNoRcv, asdu1)
		if err != nil {
			return
		}
		sf.ackNoRcv = sf.seqNoRcv
		sf.seqNoSend = (seqNo + 1) & 32767
		sf.pending = append(sf.pending, seqPending{seqNo & 32767, time.Now()})

		lc.Debugf("TX iFrame %v", iAPCI{seqNo, sf.seqNoRcv})
		sf.sendRaw <- iframe
	}

	defer func() {
		// default: STOPDT, when connected establish and not enable "data transfer" yet
		atomic.StoreUint32(&sf.isActive, inactive)
		sf.setConnectStatus(disconnected)
		checkTicker.Stop()
		_ = sf.conn.Close() // 连锁引发cancel
		sf.wg.Wait()
		sf.onConnectionLost(sf)
		lc.Debugf("run stopped!")
	}()

	sf.onConnect(sf)
	for {
		if atomic.LoadUint32(&sf.isActive) == active && seqNoCount(sf.ackNoSend, sf.seqNoSend) <= sf.option.config.SendUnAckLimitK {
			select {
			case o := <-sf.sendASDU:
				sendIFrame(o)
				idleTimeout3Sine = time.Now()
				continue
			case <-sf.ctx.Done():
				return
			default: // make no block
			}
		}
		select {
		case <-sf.ctx.Done():
			return
		case now := <-checkTicker.C:
			// check all timeouts
			if now.Sub(testFrAliveSendSince) >= sf.option.config.SendUnAckTimeout1 ||
				now.Sub(sf.startDtActiveSendSince.Load().(time.Time)) >= sf.option.config.SendUnAckTimeout1 ||
				now.Sub(sf.stopDtActiveSendSince.Load().(time.Time)) >= sf.option.config.SendUnAckTimeout1 {
				lc.Errorf("test frame alive confirm timeout t₁")
				return
			}
			// check oldest unacknowledged outbound
			if sf.ackNoSend != sf.seqNoSend &&
				//now.Sub(sf.peek()) >= sf.SendUnAckTimeout1 {
				now.Sub(sf.pending[0].sendTime) >= sf.option.config.SendUnAckTimeout1 {
				sf.ackNoSend++
				lc.Errorf("fatal transmission timeout t₁")
				return
			}

			// 确定最早发送的i-Frame是否超时,超时则回复sFrame
			if sf.ackNoRcv != sf.seqNoRcv &&
				(now.Sub(unAckRcvSince) >= sf.option.config.RecvUnAckTimeout2 ||
					now.Sub(idleTimeout3Sine) >= timeoutResolution) {
				sendSFrame(sf.seqNoRcv)
				sf.ackNoRcv = sf.seqNoRcv
			}

			// 空闲时间到，发送TestFrActive帧,保活
			if now.Sub(idleTimeout3Sine) >= sf.option.config.IdleTimeout3 {
				sf.sendUFrame(uTestFrActive)
				testFrAliveSendSince = time.Now()
				idleTimeout3Sine = testFrAliveSendSince
			}

		case apdu := <-sf.rcvRaw:
			idleTimeout3Sine = time.Now() // 每收到一个i帧,S帧,U帧, 重置空闲定时器, t3
			apci, asduVal := parse(apdu)
			switch head := apci.(type) {
			case sAPCI:
				lc.Debugf("RX sFrame %v", head)
				if !sf.updateAckNoOut(head.rcvSN) {
					lc.Errorf("fatal incoming acknowledge either earlier than previous or later than sendTime")
					return
				}

			case iAPCI:
				lc.Debugf("RX iFrame %v", head)
				if atomic.LoadUint32(&sf.isActive) == inactive {
					lc.Warnf("station not active")
					break // not active, discard apdu
				}
				if !sf.updateAckNoOut(head.rcvSN) || head.sendSN != sf.seqNoRcv {
					lc.Errorf("fatal incoming acknowledge either earlier than previous or later than sendTime")
					return
				}

				sf.rcvASDU <- asduVal
				if sf.ackNoRcv == sf.seqNoRcv { // first unacked
					unAckRcvSince = time.Now()
				}

				sf.seqNoRcv = (sf.seqNoRcv + 1) & 32767
				if seqNoCount(sf.ackNoRcv, sf.seqNoRcv) >= sf.option.config.RecvUnAckLimitW {
					sendSFrame(sf.seqNoRcv)
					sf.ackNoRcv = sf.seqNoRcv
				}

			case uAPCI:
				lc.Debugf("RX uFrame %v", head)
				switch head.function {
				//case uStartDtActive:
				//	sf.sendUFrame(uStartDtConfirm)
				//	atomic.StoreUint32(&sf.isActive, active)
				case uStartDtConfirm:
					atomic.StoreUint32(&sf.isActive, active)
					sf.startDtActiveSendSince.Store(willNotTimeout)
				//case uStopDtActive:
				//	sf.sendUFrame(uStopDtConfirm)
				//	atomic.StoreUint32(&sf.isActive, inactive)
				case uStopDtConfirm:
					atomic.StoreUint32(&sf.isActive, inactive)
					sf.stopDtActiveSendSince.Store(willNotTimeout)
				case uTestFrActive:
					sf.sendUFrame(uTestFrConfirm)
				case uTestFrConfirm:
					testFrAliveSendSince = willNotTimeout
				default:
					lc.Errorf("illegal U-Frame functions[0x%02x] ignored", head.function)
				}
			}
		}
	}
}

func (sf *Client) handlerLoop() {
	lc := sf.logger
	lc.Debugf("handlerLoop started")
	defer func() {
		sf.wg.Done()
		lc.Debugf("handlerLoop stopped")
	}()

	for {
		select {
		case <-sf.ctx.Done():
			return
		case rawAsdu := <-sf.rcvASDU:
			asduPack := asdu.NewEmptyASDU(&sf.option.params)
			if err := asduPack.UnmarshalBinary(rawAsdu); err != nil {
				lc.Warnf("asdu UnmarshalBinary failed,%+v", err)
				continue
			}
			if err := sf.clientHandler(asduPack); err != nil {
				lc.Warnf("Falied handling I frame, error: %v", err)
			}
		}
	}
}

func (sf *Client) setConnectStatus(status uint32) {
	sf.rwMux.Lock()
	atomic.StoreUint32(&sf.status, status)
	sf.rwMux.Unlock()
}

func (sf *Client) connectStatus() uint32 {
	sf.rwMux.RLock()
	status := atomic.LoadUint32(&sf.status)
	sf.rwMux.RUnlock()
	return status
}

func (sf *Client) cleanUp() {
	sf.ackNoRcv = 0
	sf.ackNoSend = 0
	sf.seqNoRcv = 0
	sf.seqNoSend = 0
	sf.pending = nil
	// clear sending chan buffer
loop:
	for {
		select {
		case <-sf.sendRaw:
		case <-sf.rcvRaw:
		case <-sf.rcvASDU:
		case <-sf.sendASDU:
		default:
			break loop
		}
	}
}

func (sf *Client) sendUFrame(which byte) {
	lc := sf.logger
	lc.Debugf("TX uFrame %v", uAPCI{which})
	sf.sendRaw <- newUFrame(which)
}

func (sf *Client) updateAckNoOut(ackNo uint16) (ok bool) {
	if ackNo == sf.ackNoSend {
		return true
	}
	// new acks validate， ack 不能在 req seq 前面,出错
	if seqNoCount(sf.ackNoSend, sf.seqNoSend) < seqNoCount(ackNo, sf.seqNoSend) {
		return false
	}

	// confirm reception
	for i, v := range sf.pending {
		if v.seq == (ackNo - 1) {
			sf.pending = sf.pending[i+1:]
			break
		}
	}

	sf.ackNoSend = ackNo
	return true
}

// IsConnected get server session connected state
func (sf *Client) IsConnected() bool {
	return sf.connectStatus() == connected
}

// clientHandler hand response handler
func (sf *Client) clientHandler(asduPack *asdu.ASDU) error {
	lc := sf.logger
	defer func() {
		if err := recover(); err != nil {
			lc.Errorf("client handler %+v", err)
		}
	}()

	lc.Debugf("ASDU %+v", asduPack)

	switch asduPack.Identifier.Type {
	case asdu.C_IC_NA_1: // InterrogationCmd
		return sf.handler.InterrogationHandler(sf, asduPack)

	case asdu.C_CI_NA_1: // CounterInterrogationCmd
		return sf.handler.CounterInterrogationHandler(sf, asduPack)

	case asdu.C_RD_NA_1: // ReadCmd
		return sf.handler.ReadHandler(sf, asduPack)

	case asdu.C_CS_NA_1: // ClockSynchronizationCmd
		return sf.handler.ClockSyncHandler(sf, asduPack)

	case asdu.C_TS_NA_1: // TestCommand
		return sf.handler.TestCommandHandler(sf, asduPack)

	case asdu.C_RP_NA_1: // ResetProcessCmd
		return sf.handler.ResetProcessHandler(sf, asduPack)

	case asdu.C_CD_NA_1: // DelayAcquireCommand
		return sf.handler.DelayAcquisitionHandler(sf, asduPack)
	}

	return sf.handler.ASDUHandler(sf, asduPack)
}

// Params returns params of client
func (sf *Client) Params() *asdu.Params {
	return &sf.option.params
}

// Send send asdu
func (sf *Client) Send(a *asdu.ASDU) error {
	if !sf.IsConnected() {
		return ErrUseClosedConnection
	}
	if atomic.LoadUint32(&sf.isActive) == inactive {
		return ErrNotActive
	}
	data, err := a.MarshalBinary()
	if err != nil {
		return err
	}
	select {
	case sf.sendASDU <- data:
	default:
		return ErrBufferFulled
	}
	return nil
}

// UnderlyingConn returns underlying conn of client
func (sf *Client) UnderlyingConn() net.Conn {
	return sf.conn
}

// Close close all
func (sf *Client) Close() error {
	sf.rwMux.Lock()
	if sf.closeCancel != nil {
		sf.closeCancel()
	}
	sf.rwMux.Unlock()
	return nil
}

// SendStartDt start data transmission on this connection
func (sf *Client) SendStartDt() {
	sf.startDtActiveSendSince.Store(time.Now())
	sf.sendUFrame(uStartDtActive)
}

// SendStopDt stop data transmission on this connection
func (sf *Client) SendStopDt() {
	sf.stopDtActiveSendSince.Store(time.Now())
	sf.sendUFrame(uStopDtActive)
}

// InterrogationCmd wrap asdu.InterrogationCmd
func (sf *Client) InterrogationCmd(coa asdu.CauseOfTransmission, ca asdu.CommonAddr, qoi asdu.QualifierOfInterrogation) error {
	return asdu.InterrogationCmd(sf, coa, ca, qoi)
}

// CounterInterrogationCmd wrap asdu.CounterInterrogationCmd
func (sf *Client) CounterInterrogationCmd(coa asdu.CauseOfTransmission, ca asdu.CommonAddr, qcc asdu.QualifierCountCall) error {
	return asdu.CounterInterrogationCmd(sf, coa, ca, qcc)
}

// ReadCmd wrap asdu.ReadCmd
func (sf *Client) ReadCmd(coa asdu.CauseOfTransmission, ca asdu.CommonAddr, ioa asdu.InfoObjAddr) error {
	return asdu.ReadCmd(sf, coa, ca, ioa)
}

// ClockSynchronizationCmd wrap asdu.ClockSynchronizationCmd
func (sf *Client) ClockSynchronizationCmd(coa asdu.CauseOfTransmission, ca asdu.CommonAddr, t time.Time) error {
	return asdu.ClockSynchronizationCmd(sf, coa, ca, t)
}

// ResetProcessCmd wrap asdu.ResetProcessCmd
func (sf *Client) ResetProcessCmd(coa asdu.CauseOfTransmission, ca asdu.CommonAddr, qrp asdu.QualifierOfResetProcessCmd) error {
	return asdu.ResetProcessCmd(sf, coa, ca, qrp)
}

// DelayAcquireCommand wrap asdu.DelayAcquireCommand
func (sf *Client) DelayAcquireCommand(coa asdu.CauseOfTransmission, ca asdu.CommonAddr, msec uint16) error {
	return asdu.DelayAcquireCommand(sf, coa, ca, msec)
}

// TestCommand  wrap asdu.TestCommand
func (sf *Client) TestCommand(coa asdu.CauseOfTransmission, ca asdu.CommonAddr) error {
	return asdu.TestCommand(sf, coa, ca)
}
