package clone

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/go-mysql-org/go-mysql/client"
)

func (d *Dumper) commandInit(ctx context.Context) error {
	payload := serializeInitCommand(cloneProtocolVersionV3, d.cfg.DDLTimeoutSec, d.cfg.BackupLock, nil)
	d.debugf("COM_INIT payload_len=%d ddl_timeout_sec=%d backup_lock=%v", len(payload), d.cfg.DDLTimeoutSec, d.cfg.BackupLock)
	if err := d.sendCommand(d.connMain, cmdInit, payload); err != nil {
		return fmt.Errorf("send COM_INIT: %w", err)
	}

	return d.readCloneResponses(ctx, d.connMain, "COM_INIT", d.handleInitResponse)
}

func (d *Dumper) commandExecute(ctx context.Context) error {
	return d.commandExecuteOn(ctx, d.connMain, d.connAux, "COM_EXECUTE")
}

func (d *Dumper) commandExecuteOn(ctx context.Context, conn *client.Conn, ackConn *client.Conn, stage string) error {
	if err := d.sendExecute(conn, stage); err != nil {
		return err
	}
	return d.readExecute(ctx, conn, ackConn, stage)
}

func (d *Dumper) sendExecute(conn *client.Conn, stage string) error {
	d.debugf("%s start", stage)
	if err := d.sendCommand(conn, cmdExecute, nil); err != nil {
		return fmt.Errorf("send %s: %w", stage, err)
	}
	return nil
}

func (d *Dumper) readExecute(ctx context.Context, conn *client.Conn, ackConn *client.Conn, stage string) error {
	handler := newExecuteResponseHandler(d, ackConn)
	return d.readCloneResponses(ctx, conn, stage, handler.handle)
}

func (d *Dumper) commandAttachWithLocators(ctx context.Context, conn *client.Conn, stage string) error {
	locators := make([][]byte, 0, len(d.locators))
	for _, loc := range d.locators {
		locators = append(locators, loc.Raw)
	}
	payload := serializeInitCommand(cloneProtocolVersionV3, d.cfg.DDLTimeoutSec, d.cfg.BackupLock, locators)
	d.debugf("%s payload_len=%d ddl_timeout_sec=%d backup_lock=%v locators=%d",
		stage, len(payload), d.cfg.DDLTimeoutSec, d.cfg.BackupLock, len(locators))
	if err := d.sendCommand(conn, cmdAttach, payload); err != nil {
		return fmt.Errorf("send %s: %w", stage, err)
	}
	if err := d.readCloneResponses(ctx, conn, stage, nil); err != nil {
		return fmt.Errorf("read %s response: %w", stage, err)
	}
	return nil
}

func (d *Dumper) commandExit(ctx context.Context, conn *client.Conn) error {
	d.debugf("COM_EXIT start")
	if err := d.sendCommand(conn, cmdExit, nil); err != nil {
		return fmt.Errorf("send COM_EXIT: %w", err)
	}

	handler := newExitResponseHandler(d)
	if err := d.readCloneResponses(ctx, conn, "COM_EXIT", handler.handle); err != nil {
		if isBenignDisconnect(err) {
			d.debugf("COM_EXIT connection closed: %v", err)
			return nil
		}
		return fmt.Errorf("read COM_EXIT response: %w", err)
	}

	return nil
}

func (d *Dumper) sendACK(ctx context.Context, conn *client.Conn, locatorIndex byte, errCode int32, descriptorBody []byte) error {
	if conn == nil {
		return nil
	}

	idx := int(locatorIndex)
	if idx < 0 || idx >= len(d.locators) {
		return fmt.Errorf("ACK locator index out of range: %d", locatorIndex)
	}

	ackPayload := serializeAckCommand(errCode, d.locators[idx].Raw, descriptorBody)
	d.debugf("COM_ACK send loc_index=%d err=%d payload_len=%d", locatorIndex, errCode, len(ackPayload))
	if err := d.sendCommand(conn, cmdAck, ackPayload); err != nil {
		return fmt.Errorf("send COM_ACK locator=%d: %w", locatorIndex, err)
	}

	if err := d.readCloneResponses(ctx, conn, "COM_ACK", func(resp cloneResponse) error {
		switch resp.Code {
		case resDataDesc, resData:
			return fmt.Errorf("unexpected %s on ACK channel", responseName(resp.Code))
		default:
			return nil
		}
	}); err != nil {
		return fmt.Errorf("read COM_ACK response: %w", err)
	}

	return nil
}

func (d *Dumper) sendStateTransitionACK(ctx context.Context, conn *client.Conn, desc descriptor) error {
	if conn == nil {
		return nil
	}

	ackDescriptor, err := buildCloneStateACKDescriptor(desc.Body)
	if err != nil {
		return fmt.Errorf("build state ACK descriptor: %w", err)
	}
	return d.sendACK(ctx, conn, desc.LocIndex, 0, ackDescriptor)
}

func (d *Dumper) handleInitResponse(resp cloneResponse) error {
	switch resp.Code {
	case resPlugin:
		key, err := parseKeyOnly(resp.Payload)
		if err != nil {
			return fmt.Errorf("parse COM_RES_PLUGIN: %w", err)
		}
		d.manifest.Plugins = append(d.manifest.Plugins, key)
		d.debugf("COM_RES_PLUGIN key=%q", key)
		return nil

	case resPluginV2:
		key, value, err := parseKeyValue(resp.Payload)
		if err != nil {
			return fmt.Errorf("parse COM_RES_PLUGIN_V2: %w", err)
		}
		d.manifest.PluginsV2 = append(d.manifest.PluginsV2, keyValue{Key: key, Value: value})
		d.debugf("COM_RES_PLUGIN_V2 key=%q value=%q", key, value)
		return nil

	case resConfig:
		key, value, err := parseKeyValue(resp.Payload)
		if err != nil {
			return fmt.Errorf("parse COM_RES_CONFIG: %w", err)
		}
		d.manifest.Configs[key] = value
		d.debugf("COM_RES_CONFIG %q=%q", key, value)
		return nil

	case resConfigV3:
		key, value, err := parseKeyValue(resp.Payload)
		if err != nil {
			return fmt.Errorf("parse COM_RES_CONFIG_V3: %w", err)
		}
		d.manifest.ConfigsV3[key] = value
		d.debugf("COM_RES_CONFIG_V3 %q=%q", key, value)
		return nil

	case resCollation:
		key, err := parseKeyOnly(resp.Payload)
		if err != nil {
			return fmt.Errorf("parse COM_RES_COLLATION: %w", err)
		}
		d.manifest.Collations = append(d.manifest.Collations, key)
		d.debugf("COM_RES_COLLATION %q", key)
		return nil

	case resLocs:
		version, locs, err := parseLocators(resp.Payload)
		if err != nil {
			return fmt.Errorf("parse COM_RES_LOCS: %w", err)
		}
		d.manifest.Protocol.Negotiated = version
		d.locators = locs

		d.manifest.Locators = d.manifest.Locators[:0]
		for i, loc := range locs {
			d.manifest.Locators = append(d.manifest.Locators, LocatorManifest{
				Index:       i,
				DBType:      loc.DBType,
				LocatorSize: uint32(len(loc.Data)),
				LocatorB64:  base64.StdEncoding.EncodeToString(loc.Data),
			})
		}
		d.debugf("COM_RES_LOCS version=0x%x locators=%d", version, len(locs))
		return nil

	default:
		d.addWarning("unknown response code=%d payload_len=%d", resp.Code, len(resp.Payload))
		return nil
	}
}

func (d *Dumper) parseDataDescriptorPayload(payload []byte, stage string) (descriptor, error) {
	desc, err := parseDescriptor(payload)
	if err != nil {
		return descriptor{}, fmt.Errorf("parse COM_RES_DATA_DESC%s: %w", stage, err)
	}
	return desc, nil
}

type processedDescriptor struct {
	descType   uint32
	expectData bool
	stateDesc  *cloneStateDescriptor
	dataDesc   *cloneDataDescriptor
}

func (d *Dumper) processDataDescriptor(desc descriptor, stage string) (*processedDescriptor, error) {
	d.mu.Lock()
	d.manifest.Stats.DescriptorPackets++
	d.mu.Unlock()

	header, err := parseCloneDescHeader(desc.Body)
	if err != nil {
		return nil, fmt.Errorf("parse clone descriptor header%s: %w", stage, err)
	}
	d.noteDescriptor(header.Type)
	d.debugf("COM_RES_DATA_DESC%s db_type=%d loc_index=%d body_len=%d desc_type=%s(%d)",
		stage, desc.DBType, desc.LocIndex, len(desc.Body), cloneDescriptorTypeName(header.Type), header.Type)

	processed := &processedDescriptor{
		descType:   header.Type,
		expectData: header.Type == cloneDescTypeData,
	}
	if header.Type == cloneDescTypeState {
		stateDesc, err := parseCloneStateDescriptor(desc.Body)
		if err != nil {
			return nil, fmt.Errorf("parse CLONE_DESC_STATE%s: %w", stage, err)
		}
		processed.stateDesc = &stateDesc
	}

	dataDesc, err := d.handleDescriptorForRestore(desc)
	if err != nil {
		return nil, fmt.Errorf("restore descriptor%s: %w", stage, err)
	}
	processed.dataDesc = dataDesc
	return processed, nil
}

func (d *Dumper) applyDataPayload(
	payload []byte,
	pendingDescriptor **descriptor,
	pendingDataDesc **cloneDataDescriptor,
	expectData *bool,
) error {
	if expectData != nil && !*expectData {
		return fmt.Errorf("received COM_RES_DATA without preceding CLONE_DESC_DATA")
	}

	d.mu.Lock()
	d.manifest.Stats.DataPackets++
	d.manifest.Stats.DataBytes += uint64(len(payload))
	d.mu.Unlock()

	if pendingDescriptor != nil {
		if *pendingDescriptor == nil {
			return fmt.Errorf("received COM_RES_DATA but pending descriptor is empty")
		}
		d.mu.Lock()
		d.manifest.Stats.DataByLocator[(*pendingDescriptor).LocIndex] += uint64(len(payload))
		d.mu.Unlock()
		d.debugf("COM_RES_DATA loc_index=%d payload_len=%d", (*pendingDescriptor).LocIndex, len(payload))
		*pendingDescriptor = nil
	}

	if d.writer != nil {
		if err := d.writer.writeData(payload); err != nil {
			return err
		}
	}
	if pendingDataDesc != nil && *pendingDataDesc != nil {
		if err := d.applyRestoreData(**pendingDataDesc, payload); err != nil {
			return err
		}
		*pendingDataDesc = nil
	}
	if expectData != nil {
		*expectData = false
		d.setPending(false, nil, nil)
	}
	return nil
}

type executeResponseHandler struct {
	dumper            *Dumper
	ackConn           *client.Conn
	pendingDescriptor *descriptor
	pendingDataDesc   *cloneDataDescriptor
	pendingExpectData bool
}

func newExecuteResponseHandler(d *Dumper, ackConn *client.Conn) *executeResponseHandler {
	return &executeResponseHandler{
		dumper:  d,
		ackConn: ackConn,
	}
}

func (h *executeResponseHandler) handle(resp cloneResponse) error {
	h.dumper.logProgress()

	switch resp.Code {
	case resDataDesc:
		return h.handleDataDescriptor(resp.Payload)
	case resData:
		return h.dumper.applyDataPayload(resp.Payload, &h.pendingDescriptor, &h.pendingDataDesc, &h.pendingExpectData)
	default:
		return h.dumper.handleInitResponse(resp)
	}
}

func (h *executeResponseHandler) handleDataDescriptor(payload []byte) error {
	desc, err := h.dumper.parseDataDescriptorPayload(payload, "")
	if err != nil {
		return err
	}

	if h.pendingExpectData || h.pendingDescriptor != nil || h.pendingDataDesc != nil {
		return fmt.Errorf("received COM_RES_DATA_DESC before expected COM_RES_DATA")
	}

	processed, err := h.dumper.processDataDescriptor(desc, "")
	if err != nil {
		return err
	}

	if processed.stateDesc != nil {
		isStart := processed.stateDesc.Flags&cloneStateFlagStart != 0
		isACK := processed.stateDesc.Flags&cloneStateFlagACK != 0
		if !isStart && !isACK {
			if err := h.dumper.sendStateTransitionACK(context.Background(), h.ackConn, desc); err != nil {
				return err
			}
		}
	} else if processed.descType == cloneDescTypeTaskMetadata {
		if err := h.dumper.sendACK(context.Background(), h.ackConn, desc.LocIndex, 0, desc.Body); err != nil {
			return err
		}
	}

	h.pendingExpectData = processed.expectData
	if processed.expectData {
		h.pendingDescriptor = &desc
		h.pendingDataDesc = processed.dataDesc
	} else {
		h.pendingDescriptor = nil
		h.pendingDataDesc = nil
	}
	h.dumper.setPending(h.pendingExpectData, h.pendingDescriptor, h.pendingDataDesc)
	return nil
}

type exitResponseHandler struct {
	dumper            *Dumper
	pendingDataDesc   *cloneDataDescriptor
	pendingExpectData bool
}

func newExitResponseHandler(d *Dumper) *exitResponseHandler {
	return &exitResponseHandler{dumper: d}
}

func (h *exitResponseHandler) handle(resp cloneResponse) error {
	switch resp.Code {
	case resData:
		return h.dumper.applyDataPayload(resp.Payload, nil, &h.pendingDataDesc, &h.pendingExpectData)
	case resDataDesc:
		desc, err := h.dumper.parseDataDescriptorPayload(resp.Payload, " during COM_EXIT")
		if err != nil {
			return err
		}
		processed, err := h.dumper.processDataDescriptor(desc, " during COM_EXIT")
		if err != nil {
			return err
		}
		h.pendingExpectData = processed.expectData
		h.pendingDataDesc = processed.dataDesc
		h.dumper.setPending(h.pendingExpectData, nil, h.pendingDataDesc)
		return nil
	default:
		return nil
	}
}

func (d *Dumper) handleDescriptorForRestore(desc descriptor) (*cloneDataDescriptor, error) {
	if d.restorer == nil {
		return nil, nil
	}

	header, err := parseCloneDescHeader(desc.Body)
	if err != nil {
		return nil, fmt.Errorf("parse clone descriptor header for restore: %w", err)
	}

	switch header.Type {
	case cloneDescTypeFileMetadata:
		fileDesc, err := parseCloneFileDescriptor(desc.Body)
		if err != nil {
			return nil, fmt.Errorf("parse CLONE_DESC_FILE_METADATA: %w", err)
		}
		if err := d.restorer.registerFile(fileDesc); err != nil {
			return nil, fmt.Errorf("restore register file metadata index=%d: %w", fileDesc.FileIndex, err)
		}
		return nil, nil

	case cloneDescTypeData:
		dataDesc, err := parseCloneDataDescriptor(desc.Body)
		if err != nil {
			return nil, fmt.Errorf("parse CLONE_DESC_DATA: %w", err)
		}
		d.debugf("restore data descriptor index=%d offset=%d data_len=%d file_size=%d",
			dataDesc.FileIndex, dataDesc.FileOffset, dataDesc.DataLen, dataDesc.FileSize)
		return &dataDesc, nil

	default:
		d.debugf("restore skip descriptor type=%s(%d) len=%d",
			cloneDescriptorTypeName(header.Type), header.Type, header.Length)
		return nil, nil
	}
}

func (d *Dumper) applyRestoreData(dataDesc cloneDataDescriptor, payload []byte) error {
	if d.restorer == nil {
		return nil
	}

	if uint32(len(payload)) != dataDesc.DataLen {
		d.addWarning("restore payload length mismatch for file index %d: descriptor=%d actual=%d",
			dataDesc.FileIndex, dataDesc.DataLen, len(payload))
	}

	written, err := d.restorer.applyData(dataDesc, payload)
	if err != nil {
		return err
	}
	d.mu.Lock()
	d.manifest.Stats.RestoredWrites++
	d.manifest.Stats.RestoredDataBytes += written
	d.mu.Unlock()
	return nil
}
