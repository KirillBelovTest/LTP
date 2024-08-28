(* ::Package:: *)


Once[Map[If[Length[PacletFind[#]] === 0, PacletInstall[#]]&][{
	"KirillBelov/CSockets", 
    "KirillBelov/Internal", 
	"KirillBelov/Objects",  
	"KirillBelov/TCP"
}]]; 


BeginPackage["KirillBelov`LTP`", {
    "KirillBelov`CSockets`", 
    "KirillBelov`Internal`", 
    "KirillBelov`Objects`", 
	"KirillBelov`TCP`"
}]; 


LTPPacketQ::usage = 
"LTPPacketQ[packet] checks that received packet is via LTP."; 


LTPPacketLength::usage = 
"LTPPacketLength[packet] returns expected length of the packet."; 


LTPHandler::usage = 
"LTPHandler[opts] LTP Handler for TCP Server."; 


LTPSend::usage = 
"LTPSend[client, message] send LTP message."; 


$LTPServer::usage = 
$LTPServer; 


Begin["`Private`"]; 


LTPPacketQ[packet_Association] := 
With[{data = packet["DataByteArray"]}, 
    Length[data] > 8 && 
    ByteArrayToString[data[[1 ;; 4]]] == "LTP#"
]; 


LTPPacketLength[packet_Association] := 
8 + ImportByteArray[packet["DataByteArray"][[5 ;; 8]], "UnsignedInteger32", ByteOrdering -> 1][[1]]; 


CreateType[LTPHandler, {
    "Responsible" -> True, 
    "Destination" -> Automatic,  
    "Deserializer" -> BinaryDeserialize, 
    "Serializer" -> BinarySerialize, 
    "MessageHandler" -> ReleaseHold
}]; 


handler_LTPHandler[packet_Association] := 
With[{
    serializer = handler["Serializer"], 
    destination = handler["Destination"], 
    data = packet["DataByteArray"]
}, 
    Module[{result = handler["MessageHandler"][handler["Deserializer"][data[[9 ;; ]]]]}, 
        If[handler["Responsible"], 
            Which[
                destination === Automatic, 
                    LTPSend[packet["SourceSocket"], result, "Serializer" -> serializer], 
                IntegerQ[destination], 
                    handler["Destination"] = CSocketConnect[destination]; 
                    LTPSend[handler["Destination"], result, "Serializer" -> serializer], 
                Head[destination] === CSocketObject, 
                    LTPSend[destination, result, "Serializer" -> serializer]
            ]
        ]
    ]
]; 


LTPHandler /: AddTo[tcp_, ltp_LTPHandler] := (
    tcp["CompleteHandler", "LTP"] = LTPPacketQ -> LTPPacketLength; 
    tcp["MessageHandler", "LTP"] = LTPPacketQ -> ltp; 
    tcp
);


Options[LTPSend] = {
    "Serializer" -> BinarySerialize
}; 


LTPSend[client_, message_, OptionsPattern[]] := 
With[{serializer = OptionValue["Serializer"]}, 
    Module[{len, data}, 
        data = serializer[message]; 
        len = ExportByteArray[Length[data], "UnsignedInteger32", ByteOrdering -> 1]; 
        BinaryWrite[client, Join[$head, len, data]]; 
    ]; 
]; 


SetAttributes[LTPEvaluate, HoldRest];


Options[LTPEvaluate] = Options[LTPSend]; 


LTPEvaluate[kernel_LTPKernelObject, code_] := 
LTPSend[kernel["Client"], Hold[code], "Serializer" -> kernel["Serializer"]]; 


CreateType[LTPKernelObject, {
    "Port", 
    "Destination", 
    "Link", 
    "Client", 
    "Serializer" -> BinarySerialize, 
    "Deserializer" -> BinaryDeserialize, 
    "Running" -> False
}]; 


linkWaitRead[link_LinkObject] := 
TimeConstrained[While[!LinkReadyQ[link], Pause[0.001]], 10]; 


linkWaitWrite[link_LinkObject?LinkReadyQ, entry: True | False: False] := 
If[entry, 
    Block[{$RecursionLimit = 20}, 
        If[Head[LinkRead[link]] === InputNamePacket && Not[LinkReadyQ[link]], 
            Null, 
            Pause[0.001]; 
            linkWaitWrite[link]
        ]
    ], 
    If[Head[LinkRead[link]] === InputNamePacket && Not[LinkReadyQ[link]], 
        Null, 
        Pause[0.001]; 
        linkWaitWrite[link]
    ]
]; 


LTPKernelLaunch[port_Integer?Positive, destination_Integer?Positive] := 
With[{kernel = LTPKernelObject[], link = LinkLaunch[First[$CommandLine] <> " -wstp"]}, 
    kernel["Port"] = port; 
    kernel["Destination"] = destination; 
    kernel["Link"] = link; 
    
    linkWaitRead[link]; 
    linkWaitWrite[link, True]; 
    LinkWrite[link, Unevaluated[EnterExpressionPacket[
        Get["KirillBelov`CSockets`"]; 
        Get["KirillBelov`Objects`"]; 
        Get["KirillBelov`TCPServer`"]; 
        Get["KirillBelov`LTP`"]; 
    ]]]; 

    linkWaitRead[link]; 
    linkWaitWrite[link, True]; 
    LinkWrite[link, Unevaluated[EnterExpressionPacket[
        KirillBelov`LTP`LTPKernelCreate[port, destination]; 
    ]]]; 

    linkWaitRead[link]; 
    linkWaitWrite[link, True]; 

    kernel["Client"] = CSocketConnect[port]; 

    kernel
]; 


LTPKernelCreate[port_Integer?Positive, destination_Integer?Positive] := 
With[{tcp = TCPServer[], ltp = LTPHandler[]}, 
    tcp += ltp; 
    ltp["Destination"] = destination; 
    SocketListen[CSocketOpen[port], tcp@Echo@#&]; 
]; 


LTPKernelObject /: Close[kernel_LTPKernelObject] := (
    Close[kernel["Client"]]
    LinkClose[kernel["Link"]]; 
); 

$head = StringToByteArray["LTP#"]; 


End[];


EndPackage[]; 
