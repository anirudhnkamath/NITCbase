# NITCbase – RDBMS Implementation Project
This project is a simple **Relational Database Management System** developed as part of an undergraduate project at NIT Calicut. 
It offers a hands-on learning experience by guiding students through building an RDBMS from scratch using an **eight-layer** modular architecture.  

### Features
* **Table management** - Create, drop, and modify tables.
* **Basic SQL support** - CREATE, DROP, ALTER, INSERT, SELECT, PROJECT, EQUI-JOIN.
* **Indexing** with B+ Trees.
* **Disk-based storage** and in-memory **buffer management**.

### Getting started
Make sure you have a C/C++ compiler (gcc/g++) to execute the code
> Note - NITCBase requires a **Linux** environment to run

1. Clone the NITCBase repository to your local machine:
   ```bash
   git clone https://github.com/anirudhnkamath/NITCbase
   ```

2. Navigate to the project directory:
   ```bash
   cd NITCBase/mynitcbase
   ```

3. Build the NITCBase application using the `make` command:
   ```bash
   make
   ```
### Warning
Commit 273bd28 makes a major change to the directory by moving the tracked directory **one level up**. All commits before commit 273bd28
are under /mynitcbase directory, and all commits after commit 273bd28 are one level above. Please work accordingly.

 
