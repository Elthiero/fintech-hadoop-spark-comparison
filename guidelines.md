# Project Guidelines: Comparative Study

## (HDFS, MapReduce vs Spark)

---

### 1. Objective

The goal of this project is to:

* Process the **same dataset using MapReduce (Python) and Spark**
* Generate insights from both approaches
* **Compare performance, complexity, and usability**

---

### 2. Group Formation

* **Group:** Use the existing groups

---

### 3. Project Workflow (COMPARATIVE MODEL)

Students must follow this structure:

#### Step 1: Data Ingestion (HDFS)

* Select a real-world dataset (~200MB or more recommended)
* Upload dataset into HDFS
* **Demonstrate:** HDFS commands, File structure

#### Step 2: Analysis using MapReduce (Python)

* **Implement:** Mapper (Using Python), Reducer (Using Python)
* Perform a meaningful analysis (e.g., aggregation, log analysis, etc.)
* **Output:**
    * Store results in HDFS
    * Present insights (e.g., totals, summaries, analysis)

#### Step 3: Analysis using Spark (on SAME DATASET)

* Load the **same original dataset from HDFS** (not MapReduce output)
* Use **PySpark**
* Perform the **same analysis logic** as MapReduce
* *Example:* If MapReduce → word count, then Spark → word count (same logic)

#### Step 4: Comparative Analysis (VERY IMPORTANT)

Students must compare MapReduce vs Spark based on:

* **A. Performance**
    * Execution time (approximate or observed)
    * Resource usage (if possible)
* **B. Code Complexity**
    * Lines of code
    * Ease of implementation
* **C. Flexibility**
    * Ease of modifying logic
    * Ability to extend analysis
* **D. Processing Model**
    * Disk-based vs. in-memory
    * Number of steps required

#### Step 5: Final Insights

* Which approach is better for this problem?
* When should MapReduce be preferred?
* When should Spark be preferred?

---

### 4. Key Requirement

Both implementations must:

* Use the **same dataset**
* Solve the **same problem**
* Produce **comparable outputs**

---

### 5. Presentation on this Sunday

Must include:

1. Introduction
2. Dataset description
3. HDFS setup
4. MapReduce implementation + output
5. Spark implementation + output
6. **Comparison table (MANDATORY)**
7. Project/Code Demonstration
