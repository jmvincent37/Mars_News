# Module 12 Challenge - NoSQL 

### Summary:
The assignment involves evaluating the ratings data of various establishments in the United Kingdom using a json file, MongoDB, and PyMongo. The goal is to assist a food magazine in analyzing the data and answering specific questions. The task is divided into three parts:

### Part 1: Database and Jupyter Notebook Setup
In this part, the data from the "establishments.json" file is imported into a NoSQL database named "uk_food" with a collection named "establishments." The data is imported using the Mongo import command. The necessary libraries, PyMongo and Pretty Print (pprint), are imported, and an instance of the Mongo Client is created. The database and collection are confirmed, and one document from the collection is displayed using the find_one function.

### Part 2: Update the Database
In this part, modifications are made to the database as per the magazine's requirements. A new restaurant called "Penang Flavours" is added to the database. The BusinessTypeID for "Restaurant/Cafe/Canteen" is found, and the new restaurant is updated with the correct BusinessTypeID. Establishments within the Dover Local Authority are removed from the database. Some number values stored as strings are converted to decimal numbers and integers using the update_many function.


### Part 3: Exploratory Analysis
In this part, exploratory analysis is performed on the database to answer specific questions provided by the magazine. The questions include finding establishments with a hygiene score of 20, establishments in London with a RatingValue greater than or equal to 4, top 5 establishments with a RatingValue of 5 sorted by lowest hygiene score and nearest to the new restaurant "Penang Flavours," and the number of establishments with a hygiene score of 0 in each Local Authority area sorted in descending order.

### Grading
The assignment provides detailed requirements for each part, including the expected commands, queries, and outputs. The submission requires a Jupyter Notebook file containing the implementation code, a GitHub repository with the code, appropriate commit messages, and well-commented code. The assignment will be graded based on meeting the specified requirements.

### Additonal Resources
Tutor Session<br />
https://stackoverflow.com<br />
https://www.geeksforgeeks.org<br />
https://mongodb.com<br />
http://tutorialspoint.com
