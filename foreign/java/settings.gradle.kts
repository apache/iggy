rootProject.name = "iggy-java-client"

include("java-sdk")

include("simple-consumer-example")
project(":simple-consumer-example").projectDir = file("examples/simple-consumer")
include("simple-producer-example")
project(":simple-producer-example").projectDir = file("examples/simple-producer")
