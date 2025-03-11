# LTP

**LTP** - **L**ength payload **T**ransfer **P**rotocol based on primitive messaging algorithm. 

## Message structure

| Byte numbers |Type         | Example    | Meaning              |
| ---          | ---         | ---        | ---                  |
| 0 - 3        |const string | "LTP#"     | Sign header          |
| 4 - 7        |number uint  | 18         | Total message length |
| 8 - ...      |byte array   | 0x00..0xFF | Payload              |

## Installation

```wolfram
PacletInstall["KirillBelov/Objects"];
PacletInstall["KirillBelov/CSockets"];
PacletInstall["KirillBelov/LTP"];
```

## Import

```wolfram
Get["KirillBelov`CSockets`"]
Get["KirillBelov`LTP`"]
```

## Simple Server

```wolfram
serverSocket = CSocketOpen[8080]; 
socketHandler = CSocketHandler[]; 

ltpHandler = LTPHandler[]; 

socketHandler["Accumulator", "LTP"] = LTPPacketQ -> LTPPacketLength; 
socketHandler["Handler", "LTP"] = LTPPacketQ -> ltpHandler; 

ltpHandler["Responsible"] = False; 
ltpHandler["Deserializer"] = ByteArrayToString; 
ltpHandler["Handler"] = Echo; 

listener = CSocketListen[serverSocket, socketHandler];
```

## Wolfram Client

```wolfram
clientSocket = SCocketConnect[8080]; 
LTPSend["hello", "Serializer" -> StringToByteArray]
```

## Buld Java Client

```sh
mvn install
```

## Java Client

```java
import kirillbelov.ltp.LTPClient; 

public class MyProgram {
    public static void main(String[] args) {
        ltpClient = new LTPClient(8080); 
        ltpClient.sendMessage("hello"); 
    }
}
```
