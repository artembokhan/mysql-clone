package clone

import (
	"encoding/hex"
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func humanBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := uint64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	prefixes := []string{"KiB", "MiB", "GiB", "TiB", "PiB"}
	return fmt.Sprintf("%.2f %s", float64(n)/float64(div), prefixes[exp])
}

// HumanBytes formats byte counts in binary units (KiB, MiB, ...).
func HumanBytes(n uint64) string {
	return humanBytes(n)
}

func commandName(code byte) string {
	switch code {
	case byte(mysql.COM_CLONE):
		return "COM_CLONE"
	case cmdInit:
		return "COM_INIT"
	case cmdAttach:
		return "COM_ATTACH"
	case cmdReinit:
		return "COM_REINIT"
	case cmdExecute:
		return "COM_EXECUTE"
	case cmdAck:
		return "COM_ACK"
	case cmdExit:
		return "COM_EXIT"
	default:
		return "UNKNOWN_CMD"
	}
}

func responseName(code byte) string {
	switch code {
	case resLocs:
		return "COM_RES_LOCS"
	case resDataDesc:
		return "COM_RES_DATA_DESC"
	case resData:
		return "COM_RES_DATA"
	case resPlugin:
		return "COM_RES_PLUGIN"
	case resConfig:
		return "COM_RES_CONFIG"
	case resCollation:
		return "COM_RES_COLLATION"
	case resPluginV2:
		return "COM_RES_PLUGIN_V2"
	case resConfigV3:
		return "COM_RES_CONFIG_V3"
	case resComplete:
		return "COM_RES_COMPLETE"
	case resError:
		return "COM_RES_ERROR"
	default:
		return "UNKNOWN_RES"
	}
}

func hexPreview(data []byte, max int) string {
	if len(data) == 0 {
		return "-"
	}
	if max <= 0 {
		max = 1
	}
	if len(data) <= max {
		return hex.EncodeToString(data)
	}
	return fmt.Sprintf("%s...(+%d bytes)", hex.EncodeToString(data[:max]), len(data)-max)
}
