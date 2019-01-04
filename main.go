//==============================================================================
// Project name: Widget Production line
// Author: Quan Bui
// Date: 12/26/2018
// File: main.go
//==============================================================================

package main

import (
    "fmt"
    "flag"
    "time"
    "math/rand"
    "bytes"
    "strconv"
    "sync"
)

const ASCII = "abcdefghijklmnopqrstuvxyz0123456789"
const ID_LENGTH = 32
const TIME_FORMAT = "15:04:05.000000"

var wg sync.WaitGroup

type widget struct {
    id      string      // Universally unique
    source  string      // Which producer created this widget
    time    time.Time   // Time set by producer when widget was created
    broken  bool        // Widget is broken or not
}

//=============================================================================
func idMaker() string {
    var buffer bytes.Buffer

    for i := 0; i < ID_LENGTH; i++ {
        if i == ID_LENGTH / 2 {
            buffer.WriteString("-")
        } else {
            buffer.WriteString(string(ASCII[rand.Intn(len(ASCII))]))
        }
    }
    return buffer.String()
}

type producer struct {
    name        string
    numWidgets  int
}

// The process when a producer produces a widget
func produce(prod producer, broken bool) widget {
    toReturnWidget := widget{idMaker(), prod.name, time.Now(), broken}
    //fmt.Printf("Producing [id=%s source=%s time=%s broken=%t]\n",
    //toReturnWidget.id, toReturnWidget.source, toReturnWidget.time.Format(TIME_FORMAT), toReturnWidget.broken)
    return toReturnWidget
}

//=============================================================================
type consumer struct {
    name    string
}

func consume(con consumer, wid widget) bool{
    fmt.Printf("%s ", con.name)
    if !wid.broken {
        fmt.Printf("consumes")
    } else {
        fmt.Printf("found a broken widget ")
    }
    fmt.Printf("[id=%s source=%s time=%s ", wid.id, wid.source, wid.time.Format(TIME_FORMAT))
    if !wid.broken {
        fmt.Printf("broken=%t] in %s time\n", wid.broken, time.Since(wid.time))
    } else {
        fmt.Printf("broken=%t] -- stopping production\n", wid.broken)
    }

    return wid.broken
}

//=============================================================================
// ProductionLine should be a producer produces following by a consumer consumes
func widgetProductionLine(widgetTable []widget, numWidgets int, numProducers int, numConsumers int, numKth int) {
    var producerTable []producer
    // Make all the producers first
    for i:= 0; i < numProducers; i++ {
        var buffer bytes.Buffer
        buffer.WriteString("producer_")
        buffer.WriteString(strconv.Itoa(i))
        producerTable = append(producerTable, producer{buffer.String(), 0})
    }

    var consumerTable []consumer
    // Make all the consumers 
    for i:= 0; i < numConsumers; i++ {
        var buffer bytes.Buffer
        buffer.WriteString("consumer_")
        buffer.WriteString(strconv.Itoa(i))
        consumerTable = append(consumerTable, consumer{buffer.String()})
    }
    brokenWidgetConsumed := false                   // Bool var indicates if brokenWidget is consumed 
    brokenWidgetChannel := make(chan bool)          // This channel used to signal when broken widget is produced
    productionCounterChannel := make(chan int)      // This channel used to pass the production counter
    consumptionCounterChannel := make(chan int)     // This channel used for pass consumption counter
    quitChannel := make(chan bool, len(producerTable) + len(consumerTable))   // This buffered channel used to send quit signal(s) 
    widgetChannel := make(chan widget)

    // Initialization of the brokenWidgetChannel
    go func() {
        wg.Add(1)
        if numKth == 0 {
            brokenWidgetChannel <- true
        }   else {
            brokenWidgetChannel <- false
        }
        wg.Done()
    }()

    // Multithreading production line
    for j := 0; j < len(producerTable); j++ {
        //fmt.Println("j=", j)

        wg.Add(1)
        go func(j int) {
            for {
                select {
                case <-quitChannel:
                    //fmt.Printf("Production line %d is shutting down...\n", j)
                    wg.Done()
                    return
                case broken := <-brokenWidgetChannel:
                    workingWidget := produce(producerTable[j], broken)
                    widgetTable = append(widgetTable, workingWidget)
                    productionCounterChannel <- 1
                    widgetChannel <- workingWidget      // Sending the widget for consumption
                default:
                    //fmt.Println("No signal from brokenWidgetChannel.")
                }
            }
        }(j)
    }

    // Multithreading consumption line
    for k := 0; k < len(consumerTable); k++ {
        //fmt.Println("k=", k)

        wg.Add(1)
        go func(k int) {
            for {
                select {
                case <-quitChannel:
                    //fmt.Printf("Consumption line %d is shutting down...\n", k)
                    wg.Done()
                    return
                case widget := <-widgetChannel:
                    if consume(consumerTable[k], widget) {
                        brokenWidgetConsumed = true
                    }
                    consumptionCounterChannel <- 1
                default:
                    //fmt.Println("widgetChannel empty. Waiting for widget to be produced")
                }
            }
        }(k)
    }

    // Production Manager. This doesn't have to be a goroutine
    // Needs to constantly checking if counter exceeds numWidgets
    productionCounter := 0
    consumptionCounter := 0
    for {
        productionCounter += <-productionCounterChannel
        consumptionCounter += <-consumptionCounterChannel
        //fmt.Printf("Producing %d widgets.\n", productionCounter)
        //fmt.Printf("Consuming %d widgets.\n", consumptionCounter)

        // Signaling when to stop: 
        // - When all widgets are produced by producers AND when all widgets are consumed by consumers
        // - OR When a broken widget is consumed (HIGHEST PRIORITY)
        if brokenWidgetConsumed || (productionCounter == numWidgets && consumptionCounter == numWidgets) {
            //fmt.Println("Sending quit signals")
            for i := 0; i < len(producerTable) + len(consumerTable); i++ {
                quitChannel <- true
            }

            // Closing all channels, except for quitChannel
            //fmt.Println("Closing all channels")
            fmt.Println("[execution stops]")
            close(productionCounterChannel)
            close(consumptionCounterChannel)
            close(brokenWidgetChannel)
            close(widgetChannel)
            break
        }

        // Signaling if the widget will be broken when produced
        if productionCounter == numKth {
            brokenWidgetChannel <- true
        } else {
            brokenWidgetChannel <- false
        }
    }
}

func main() {
    rand.Seed(time.Now().UnixNano())

    var numWidgets = flag.Int("n", 10, "Sets the number of widgets created")
    var numProducers = flag.Int("p", 1, "Sets the number of producers created")
    var numConsumers = flag.Int("c", 1, "Sets the number of consumers created")
    var numKth = flag.Int("k", -1, "Sets the kth widget to be broken")
    flag.Parse()

    var widgetProductionTable []widget

    //fmt.Println("numWidgets:", *numWidgets)
    //fmt.Println("numProducers:", *numProducers)
    //fmt.Println("numConsumers:", *numConsumers)
    //fmt.Println("numKth:", *numKth)

    widgetProductionLine(widgetProductionTable, *numWidgets, *numProducers, *numConsumers, *numKth - 1)
    wg.Wait()
}
