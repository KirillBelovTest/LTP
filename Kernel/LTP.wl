(* ::Package:: *)

BeginPackage["KirillBelov`LTP`", {"KirillBelov`Objects`", "JLink`"}]; 


LTPPacketQ::usage = 
"LTPPacketQ[packet] checks that received packet is via LTP."; 


LTPPacketLength::usage = 
"LTPPacketLength[packet] returns expected length of the packet."; 


LTPHandler::usage = 
"LTPHandler[opts] LTP Handler for TCP Server."; 


LTPSend::usage = 
"LTPSend[client, message] send LTP message."; 


Begin["`Private`"]; 


LTPPacketQ[packet_Association] := 
With[{data = packet["DataByteArray"]}, 
    Length[data] > 8 && 
    ByteArrayToString[data[[1 ;; 4]]] == "LTP#"
]; 


LTPPacketLength[packet_Association] := 
ImportByteArray[packet["DataByteArray"][[5 ;; 8]], "UnsignedInteger32", ByteOrdering -> 1][[1]]; 


CreateType[LTPHandler, {
    "Responsible" -> True, 
    "Destination" -> Automatic,  
    "Deserializer" -> BinaryDeserialize, 
    "Serializer" -> BinarySerialize, 
    "MessageHandler" -> ReleaseHold
}]; 


LTPHandler /: (handler_LTPHandler)[packet_Association] := 
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
                True, 
                    LTPSend[destination, result, "Serializer" -> serializer]
            ]
        ]
    ]
]; 


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


$directory = DirectoryName[$InputFileName, 2]; 


$head = StringToByteArray["LTP#"]; 


Map[AddToClassPath] @ 
Map[Last] @ 
GroupBy[StringRiffle[StringSplit[#, "-"][[;; -2]], "-"]&] @ 
FileNames["*.jar", {FileNameJoin[{$directory, "Java"}]}]; 


End[];


EndPackage[]; 
