# 332 Project
### Team navy 
[@Hyunsoo1030](https://github.com/Hyunsoo1030)
(surgeon), [@Octaki](https://github.com/Octaki), [@Matthew2839](https://github.com/Matthew2839)

### Project info
The goal of this project is to build a distributed sorting system.

### Development environment
scala version: 2.13.16

### How to use 
1. First, you need to clone our repository.  
`git clone https://github.com/Hyunsoo1030/332project.git`  
2. Then, build the project & make jar files
```commandline
sbt clean
sbt compile
sbt master/assembly
sbt worker/assembly
```
3. Execute the Master server  
`master [# of workers]`
4. Execute the Workers  
   `worker 2.2.2.254:8915 -I [directory0] [directory1] ... -O [output direcotry]`  
Or you can just use `smalltest [# of workers]`, `bigtest [# of workers]`, and `largetest [# of workers]` in another terminal of Master to execute this program.
