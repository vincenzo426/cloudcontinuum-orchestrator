package datatransfer

// TransferInfo contains information about a data transfer operation
type TransferInfo struct {
	SourceCluster   string
	TargetCluster   string
	DataSize        string
	NetworkLatency  int64
	TransferTime    int64
	TransferDetails string
}

// NoTransferNeeded returns a TransferInfo indicating no transfer is required
func NoTransferNeeded() *TransferInfo {
	return &TransferInfo{
		NetworkLatency:  0,
		TransferTime:    0,
		TransferDetails: "No data transfer required (data locality satisfied)",
	}
}
