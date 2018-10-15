// Learn more about F# at http://fsharp.org

open System
open Microsoft.Azure.ServiceBus
open System.Text
open System.Threading.Tasks
open System.Threading
open FSharp.Control.Tasks.V2.ContextInsensitive

let sendMessage (connectionString :string) queueName sessionId messageId label (message :string) =
    async {
        let queueClient = QueueClient(connectionString, queueName)
        try 
            let message = Message(Encoding.UTF8.GetBytes message) 
            message.CorrelationId <- Guid.NewGuid().ToString()
            message.SessionId <- sessionId
            message.MessageId <- messageId
            message.ContentType <- "application/json"
            message.Label <- label
            return! queueClient.SendAsync(message) |> Async.AwaitTask
            do! queueClient.CloseAsync() |> Async.AwaitTask
        with e ->
            printf "%s" e.Message
            do! queueClient.CloseAsync() |> Async.AwaitTask
    }

let printMessage name queueName sessionId messageId message =
    task {
        printfn 
            """
                ####################### %s ######################
                Queue: %s. 
                SessionId: %s
                MessageId: %s
                Message: %s
                #################################################
            """
            name
            queueName
            sessionId
            messageId
            message
        let random = Random()

        return random.Next(0,100) > 5 }
    
let receiveMessage name (connectionString : string) =

    let exceptionReceivedHandler (args : ExceptionReceivedEventArgs) =
        let context = args.ExceptionReceivedContext
        let error = 
            sprintf "Error receiving a message from Azure Service Bus. Context is:\r\nAction: %s\r\nClientId: %s\r\nEndpoint: %s\r\nEntityPath: %s"
                context.Action
                context.ClientId
                context.Endpoint
                context.EntityPath

        printf """%s\r\n%A""" error args.Exception.Message

        Task.CompletedTask
    
    let processMessage (queueClient : IQueueClient) (session : IMessageSession) (message : Message) (_ : CancellationToken) =
        task {
            let! r = printMessage name queueClient.QueueName session.SessionId message.MessageId (Encoding.UTF8.GetString message.Body)
            match r with
            | true ->
                return! session.CompleteAsync(message.SystemProperties.LockToken)
                if message.MessageId.Contains("Msg3") 
                then return! session.CloseAsync()
            | false -> return! session.AbandonAsync(message.SystemProperties.LockToken)
            
            do! Task.Delay(5000)
        } :> Task

    let f queueName =
        let queueClient = QueueClient(connectionString, queueName)
        try
            let sessionHandlerOptions =  new SessionHandlerOptions(fun x -> exceptionReceivedHandler x)
            sessionHandlerOptions.MaxConcurrentSessions <- 1
            sessionHandlerOptions.AutoComplete  <- false
            let processMessage = processMessage queueClient
            queueClient.RegisterSessionHandler(processMessage, sessionHandlerOptions)
        with e -> printfn "ERROR RegisterSessionHandler: %O"  e
        
    f


let sendKingMessages connectionString queueName =
    async {    
        do! sendMessage connectionString queueName "King's Session" "Msg1/King" "This is message 1" "I'm taking to you from heaven (or cloud??)"
        // same message deduplicated
        do! sendMessage connectionString queueName "King's Session" "Msg1/King" "This is message 1" "I'm taking to you from heaven (or cloud??)"        
        do! sendMessage connectionString queueName "King's Session" "Msg1/King" "This is message 1" "I'm taking to you from heaven (or cloud??)"
        
        do! sendMessage connectionString queueName "King's Session" "Msg2/King" "This is message 2" "Still talking"
        do! sendMessage connectionString queueName "King's Session" "Msg3/King" "This is message 3" "You don't hear me!"
    }

let sendQueenMessages connectionString queueName =
    async {
        do! sendMessage connectionString queueName "Queen's Session" "Msg1/Queen" "This is message 1" "I'm taking to you from heaven (or cloud??)"
        // same message deduplicated
        do! sendMessage connectionString queueName "Queen's Session" "Msg1/Queen" "This is message 1" "I'm taking to you from heaven (or cloud??)"    
        do! sendMessage connectionString queueName "Queen's Session" "Msg2/Queen" "This is message 2" "Still talking"
        do! sendMessage connectionString queueName "Queen's Session" "Msg3/Queen" "This is message 3" "I don't hear you"
    }

let sendKingdomMessages connectionString queueName=
    async {
        do! Task.Delay(2000) |> Async.AwaitTask
        do! sendQueenMessages connectionString queueName
        do! sendKingMessages connectionString queueName
    }


[<EntryPoint>]
let main _ =
    printfn "Hello Microsoft Azure Service bus from F#!"
    let connectionString = "Endpoint=sb://<name space>.servicebus.windows.net/;SharedAccessKeyName=<shared access key name>;SharedAccessKey=<shared access key>"
    let queueName = "kingdom_messenger"
    
    receiveMessage "receiver 1" connectionString queueName
    receiveMessage "receiver 2" connectionString queueName
    
    sendKingdomMessages connectionString queueName
    |> Async.RunSynchronously

    Console.ReadLine() |> ignore
    0 // return an integer exit code
