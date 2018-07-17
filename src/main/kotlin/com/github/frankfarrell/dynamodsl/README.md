# Dynamo DSL
A Kotlin internal DSL for querying DynamoDB. 

Currently it supports Query, with plans to implement Scan and Update soon. 

## Usage

Allows nesting of conditionals in a filter query in a type safe way
```kotlin 
val result = DynamoDSL().query("mytable") { 
            hashKey("myHashKey") {
                eq(2)
            }
            sortKey("mysortkey"){
                between ( 2 AND 3)
            }
            filtering {
                attribute("age") {
                    eq(44)
                } and attribute("weight"){
                    eq(14)
                } or attribute("weight"){
                    eq(14)
                } or {
                    attribute(value = "age") {
                        eq(14)
                    } and {
                        attribute("weight") {
                            eq("12")
                        } or attribute("colour"){
                            eq("blue")
                        }
                    }
                }
            }
        }
        
if(result.hasNext()){
    println(result.next())
}

```