#!/usr/bin/env wolframscript

If[$FrontEnd === Null, 
    PacletDirectoryLoad[DirectoryName[$InputFileName]], 
    PacletDirectoryLoad[NotebookDirectory[]]
];


Get["KirillBelov`CSockets`TCP`"];


Get["KirillBelov`CSockets`Handler`"];


Get["KirillBelov`LTP`"];


socket = CSocketOpen[8080]; 


handler = CSocketHandler[];


ltp = LTPHandler[
    "Responsible" -> False, 
    "Deserializer" -> ByteArrayToString, 
    "Serializer" -> StringToByteArray, 
    "Handler" -> Echo
];


handler["Accumulator", "LTP"] = LTPPacketQ -> LTPPacketLength;


handler["Handler", "LTP"] = LTPPacketQ -> ltp;


listener = SocketListen[socket, handler, "SelectTimeout" -> 10]; 


While[True, Pause[1]];