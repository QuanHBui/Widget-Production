# WidgetProduction
Small project to learn golang
This was used as a small project for the interview for PlatoApp backend engineer position. 

# exercise

## Abstract

We consider a `widget` type that is produced by a `producer` and consumed by a `consumer`.  

We will produce `N` widgets exactly across all `producers`, regardless of how many or few producers there are (some
producers may end up producing more or less than others, based on timing).

Some widgets may be broken, causing all `producers` to stop creating widgets prior to producing all `N` widgets.

## Widgets

Consider the following definition for a `widget`:

```
type widget struct {
	id     string
	source string
	time   time.Time
	broken bool
}
```

- A widget's id *must* be universally unique: that is, unique even across multiple test runs.
- A widget's source is a human readable description of which `producer` created the widget, e.g. `producer_91` 
- A widget's time is set by the `producer` to be when the widget was created.
- A widget is "good" -- that is, `broken==false`, by default.

When a `consumer` consumes a widget, it should print to standard out:

1. Which consumer consumed it: e.g. `consumer_1`
2. The id of the consumed widget: e.g. `91f023ae62e42b44-af6103c770dc1dda`
3. The source producer that built the widget: e.g. `producer_96`
4. The time the producer built the widget: e.g. `21:37:21.177660`
5. If the widget was broken or not: e.g. `false`
6. The difference in time (as a duration) between when the consumer consumed the widget and
   when the producer made it: e.g. `16.865µs` 

As a full example, when a consumer consumes a widget, print out:

```
consumer_55 consumes [id=91f023ae62e42b44-af6103c770dc1dda source=producer_71 time=22:20:21.616849 broken=false] in 23.702µs time
```

## Broken Widgets

Unfortunately, some widgets break when they are produced.  When the `K`th widget is produced (`0 < K <= N`), the
`producer` that creates the widget marks it as `broken=true`.  

When any `consumer` encounters a broken widget, it must:

1. Print out that it encountered a broken widget
2. Signal the entire production line of `producers` that they should stop

As an example:

```
$ go run main.go -n 10 -p 1 -c 1 -k 3
consumer_0 consumes [id=df51bb4dc9508ea9-fcaf35440bba015f source=producer_0 time=22:22:33.343378 broken=false] in 18.593µs time
consumer_0 consumes [id=2fb200b030f621cd-c892ca4930d3f0de source=producer_0 time=22:22:33.343383 broken=false] in 168.874µs time
consumer_0 found a broken widget [id=0e572d41183e54ec-3375687e937bc434 source=producer_0 time=22:22:33.343384 broken=true] -- stopping production
[execution stops]
```

Note that for different implementations, the shutdown may not be immediate or it may even be too late: producers may be
already producing the final remaining widgets despite a call for shutting down.    

## Program

Create a CLI program to run the simulation.

Allow for the command line parameters:

| Option | What it does                         | Default value              |
|--------|--------------------------------------|----------------------------|
| `-n`   | Sets the number of widgets created   |   `10`                     |
| `-p`   | Sets the number of producers created |   `1`                      |
| `-c`   | Sets the number of consumers created |   `1`                      |
| `-k`   | Sets the `k`th widget to be broken   |   `-1` (no broken widgets) |

Example for 1000 widgets, produced by 50 producers, consumed by 7 consumers.

```
go run main.go -n 1000 -p 50 -c 7
```

## Notes

- Use no packages from outside standard Go standard libraries (no third party frameworks, libraries, etc)
