# MapReduce-BigData

Using hadoop/mapreduce to analyze social network data.

Folder:Dataset
Input:
Input files 
1. soc-LiveJournal1Adj.txt 
The input contains the adjacency list and has multiple lines in the following format:
<User><TAB><Friends>
2. userdata.txt 
The userdata.txt contains dummy data which consist of 
column1 : userid
column2 : firstname
column3 : lastname
column4 : address
column5: city
column6 :state
column7 : zipcode
column8 :country
column9 :username
column10 : date of birth.


Folder: bigdata1

A MapReduce program in Hadoop that implements a simple “Mutual/Common friend list of two friends". The key idea is that if two people are friend then they have a lot of mutual/common friends. This program will find the common/mutual friend list for them.

For example,
Alice’s friends are Bob, Sam, Sara, Nancy
Bob’s friends are Alice, Sam, Clara, Nancy
Sara’s friends are Alice, Sam, Clara, Nancy

As Alice and Bob are friend and so, their mutual friend list is [Sam, Nancy]
As Sara and Bob are not friend and so, their mutual friend list is empty. (In this case you may exclude them from your output).

Here, <User> is a unique integer ID corresponding to a unique user and <Friends> is a comma-separated list of unique IDs corresponding to the friends of the user with the unique ID <User>. Note that the friendships are mutual (i.e., edges are undirected): if A is friend with B then B is also friend with A. The data provided is consistent with that rule as there is an explicit entry for each side of each edge. So when you make the pair, always consider (A, B) or (B, A) for user A and B but not both.
Output: The output should contain one line per user in the following format:
<User_A>, <User_B><TAB><Mutual/Common Friend List>
where <User_A> & <User_B> are unique IDs corresponding to a user A and B (A and B are friend). < Mutual/Common Friend List > is a comma-separated list of unique IDs corresponding to mutual friend list of User A and B.

Folder: bigdata2

Any two Users (they are friend) as input, output the list of the user id of their mutual friends.
Output format:
UserA, UserB list userid of their mutual Friends.

Folder:bigdata3

Use in-memory join 
Given any two Users (they are friend) as input, output the list of the names and the date of birth (mm/dd/yyyy) of their mutual friends.
Note: use the userdata.txt to get the extra user information.
Output format:
UserA id, UserB id, list of [names: date of birth (mm/dd/yyyy)] of their mutual Friends.

Sample Output:
1234     4312       [John:12/05/1985, Jane : 10/04/1983, Ted: 08/06/1982]

Folder: bigdata4

Reduce-side join and job chaining:
Step 1: Calculate the maximum age of the direct friends of each user.
Step 2: Sort the users by the calculated maximum age from step 1 in descending order.
Step 3. Output the top 10 users from step 2 with their address and the calculated maximum age.

Sample output.
User A, 1000 Anderson blvd, Dallas, TX, maximum age of direct friends.



