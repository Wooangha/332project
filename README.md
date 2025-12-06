# cs332-project
repo for cs332 project; distributed sorting with fault tolerance

[Our Notion Web Page](https://wooangha.notion.site/CSED-332-project-Home-2a6a179b8de7815aa7e1e478cf8cdd5a?source=copy_link)

## Setup(Build) Project
```bash
git clone https://github.com/Wooangha/332project.git
cd 332project/distributed-sorting
sbt compile
```

## How to run Master
```bash
./master <#-of-workers>
```

## How to run Worker
```bash
./worker <ip:port> -I <input-directory> ... -O <output-directory>
```
