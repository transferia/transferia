Source (Storage) UML schema:

@startuml
skinparam sequenceMessageAlign center

Participant Activate
Participant GpfdistStorage
Participant PipeReader
Participant GpfdistBin
Participant Pipe
Participant ExternalTable

activate GpfdistStorage

Activate -> GpfdistStorage: LoadTable(pusher)

GpfdistStorage -> GpfdistBin: Init GpfdistBin
activate GpfdistBin

GpfdistBin -> Pipe: []syscall.MkFifo(file)
activate Pipe

GpfdistStorage -> PipeReader: Create PipeReader
activate PipeReader
GpfdistStorage --> PipeReader: Run(pusher)

PipeReader -> GpfdistBin: Open pipe as read only

GpfdistBin -> Pipe: []os.OpenFile(pipe)
GpfdistBin <- Pipe: []os.File
PipeReader <- GpfdistBin: []os.File

PipeReader -> Pipe: Read pipe

GpfdistStorage -> GpfdistBin: Start read through external table

GpfdistBin -> ExternalTable: Create writable external table and start insert
activate ExternalTable

Pipe <-- ExternalTable: TSV Data
PipeReader <-- Pipe: TSV Data
PipeReader --> PipeReader: pusher(TSV Data)

ExternalTable -> GpfdistBin: Exported rows count
deactivate ExternalTable
GpfdistBin -> GpfdistStorage: Exported rows count

GpfdistStorage -> PipeReader: Wait for result

PipeReader -> Pipe: Close pipe

GpfdistStorage <-- PipeReader: Pushed rows count

deactivate PipeReader

GpfdistStorage -> GpfdistBin: Stop

GpfdistBin -> Pipe: []os.Remove(pipe)
deactivate GpfdistBin

GpfdistStorage -> Activate: Result
deactivate GpfdistStorage
