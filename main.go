//==============================================================================
// Project name: Widget Production line
// Author: Quan Bui
// Date: 01/14/2019
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

//==============================================================================
type Widget struct {
    id      string      // Universally unique
    source  string      // Which Producer created this Widget
    time    time.Time   // Time set by Producer when Widget was created
    broken  bool        // Widget is broken or not
}

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

//==============================================================================
type Producer struct {
    name        string
}

// The process when a Producer produces a Widget
func (prod Producer) produce(broken bool) Widget {
    return Widget{idMaker(), prod.name, time.Now(), broken}
}

// jobChannel will be used to keep track of how many widgets got produced, and which widget is broken
func productionLine(producerTable []Producer, numWidgets int, numKth int, jobChannel <-chan int, outWidgetChannel chan<- Widget, quitChannel <-chan struct{}) {
    defer wg.Done()
    defer close(outWidgetChannel)
    var productionWaitGroup sync.WaitGroup

    productionWaitGroup.Add(len(producerTable))
    for _, workingProducer := range producerTable {
        go func(workingProducer Producer) {
            defer productionWaitGroup.Done()
            for i := range jobChannel {
                select {
                default:
                    if (numKth == i) {
                        // Produce broken widget if i = numKth
                        outWidgetChannel <- workingProducer.produce(true)
                    } else {
                        outWidgetChannel <- workingProducer.produce(false)
                    }
                case <-quitChannel:
                    return
                }
            }
        }(workingProducer)
    }
    productionWaitGroup.Wait()
}

//==============================================================================
type Consumer struct {
    name string
}

func (con Consumer) consume(wid Widget) bool {
    if !wid.broken {
        fmt.Printf("%s consumes [id=%s source=%s time=%s broken=%t] in %s time\n",
            con.name, wid.id, wid.source, wid.time.Format(TIME_FORMAT), wid.broken, time.Since(wid.time))
    } else {
        fmt.Printf("%s found a broken widget [id=%s source=%s time=%s broken=%t] -- stopping production\n",
            con.name, wid.id, wid.source, wid.time.Format(TIME_FORMAT), wid.broken)
    }
    return wid.broken
}

// Consumer will quit working once the widgetChannel is closed
func consumptionLine(consumerTable []Consumer, inWidgetChannel <-chan Widget, brokenWidgetChannel chan<- struct{}) {
    defer wg.Done()
    var consumptionWaitGroup sync.WaitGroup
    doneChannel := make(chan struct{})

    consumptionWaitGroup.Add(len(consumerTable))
    for _, workingConsumer := range consumerTable {
        go func(workingConsumer Consumer) {
            defer consumptionWaitGroup.Done()
            for workingWidget := range inWidgetChannel {
                select {
                case <-doneChannel:
                    return
                default:
                    if (workingConsumer.consume(workingWidget)) {
                        close(brokenWidgetChannel)      // brokenWidgetChannel used to signify a broken widget has been encountered
                        close(doneChannel)              // doneChannel to let the rest of the consumers knows that they need to stop
                        return
                    }
                }
            }
        }(workingConsumer)
    }
    consumptionWaitGroup.Wait()
}

//=============================================================================
// ProductionLine should be a Producer produces following by a consumer consumes
func WidgetProductionConsumptionLine(numWidgets int, numProducers int, numConsumers int, numKth int) {
    // Make all the Producers first
    var producerTable []Producer
    for i := 0; i < numProducers; i++ {
        var buffer bytes.Buffer
        buffer.WriteString("producer_")
        buffer.WriteString(strconv.Itoa(i))
        producerTable = append(producerTable, Producer{buffer.String()})
    }

    // Make all the consumers
    var consumerTable []Consumer
    for i := 0; i < numConsumers; i++ {
        var buffer bytes.Buffer
        buffer.WriteString("consumer_")
        buffer.WriteString(strconv.Itoa(i))
        consumerTable = append(consumerTable, Consumer{buffer.String()})
    }

    jobChannel := make(chan int, numWidgets)        // Job channel to keep track of how many widgets produced and which widget would be broken
    widgetChannel := make(chan Widget, numWidgets)  // Widget channel to send to consumers to consume
    quitChannel := make(chan struct{})              // To signify when the consumptionLine and productionLine will quit
    brokenWidgetChannel := make(chan struct{})      // Written by a consumer when a broken widget is met

    // Rack up all the jobs first
    for i := 1; i <= numWidgets; i++ {
        jobChannel <- i
    }
    close(jobChannel)

    wg.Add(2)
    // Producers will then grab job requests from jobChannel and produce
    go productionLine(producerTable, numWidgets, numKth, jobChannel, widgetChannel, quitChannel)

    // Consumers grabbing widgets from widget channel and consume
    go consumptionLine(consumerTable, widgetChannel, brokenWidgetChannel)

    // When brokenWidgetChannel is closed by a consumer, this will close the quitChannel to tell consumptionLine and productionLine to stop
    if (numKth > 0) {
        <-brokenWidgetChannel
        fmt.Println("[execution stops]")
        close(quitChannel)
    }
    wg.Wait()
}

func main() {
    timeBegin := time.Now()
    rand.Seed(time.Now().UnixNano())

    var numWidgets = flag.Int("n", 10, "Sets the number of Widgets created")
    var numProducers = flag.Int("p", 1, "Sets the number of Producers created")
    var numConsumers = flag.Int("c", 1, "Sets the number of consumers created")
    var numKth = flag.Int("k", -1, "Sets the kth Widget to be broken")
    flag.Parse()

    WidgetProductionConsumptionLine(*numWidgets, *numProducers, *numConsumers, *numKth)
    fmt.Printf("The program took [ %s ] to finish.\n", time.Since(timeBegin).String())
}
