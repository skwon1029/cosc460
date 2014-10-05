/* COSC 460 Fall 2014, Lab 3 */

/* These set the output format.  Please be sure to leave these settings as is. */
.header OFF
.mode list 

/* For each of the queries below, put your SQL in the place indicated by the comment.  
   Be sure to have all the requested columns in your answer, in the order they are 
   listed in the question - and be sure to sort things where the question requires 
   them to be sorted, and eliminate duplicates where the question requires that.   
   I will grade the assignment by running the queries on a test database and 
   eyeballing the SQL queries where necessary.  I won't grade on SQL style, but 
   we also won't give partial credit for any individual question - so you should be 
   confident that your query works. In particular, your output should match 
   the example output.
*/

/* Q1 -  Find the titles of all movies directed by Steven Spielberg.  */
select " ";
select "Q1";

/* Put your SQL for Q1 below */
select title from Movie 
where director = "Steven Spielberg";


/* Q2 -  Find all years that have a movie that received a rating of 4 or 5, 
         and sort them in increasing order.             
*/
select " ";
select "Q2";

/* Put your SQL for Q2 below */
select distinct year from Movie,Rating 
where Movie.mID=Rating.mID AND Rating.stars>3 
order by year;

/* Q3 -  Find the titles of all movies that have no ratings.
*/
select " ";
select "Q3";

/* Put your SQL for Q3 below */
select title from Movie
where mID not in (select mID from Rating);

/* Q4 -  Write a query to return the ratings data in a more 
         readable format: reviewer name, movie title, stars, and ratingDate. 
         Also, sort the data, first by reviewer name, then by movie title, 
         and lastly by number of stars, all in ascending order.
*/
select " ";
select "Q4";

/* Put your SQL for Q4 below */
select name,title,stars,ratingDate from Movie,Reviewer,Rating
where Reviewer.rID=Rating.rID AND Movie.mID=Rating.mID
order by name,title,stars;

/* Q5 -  For all cases where the same reviewer rated the same movie twice 
         and gave it a higher rating the second time, return the reviewer's 
         name and the title of the movie.
*/
select " ";
select "Q5";

/* Put your SQL for Q5 below */
select name,title from Movie,Reviewer,Rating R1,Rating R2
where R1.rID=R2.rID AND R1.mID=R2.mID AND R1.stars<R2.stars
AND R1.ratingDate<R2.ratingDate AND Reviewer.rID=R1.rID AND Movie.mID=R1.mID;

/* Q6 - For each movie that has at least one rating, find the highest number 
        of stars that movie received. Return the movie title and number of 
        stars. Sort by movie title. 
*/
select " ";
select "Q6";

/* Put your SQL for Q6 below */
select distinct title,stars from Movie,Rating R1
where Movie.mID=R1.mID
AND not exists (select * from Rating R2 where R1.mID=R2.mID AND R2.stars>R1.stars)
order by title;

/* Q7 - For each movie, the title along with the number of ratings it has 
        received.  Your result should include those movies that have zero ratings.                                                                 
*/
select " ";
select "Q7";

/* Put your SQL for Q7 below */
select title,count(*) from Movie, Rating
where Movie.mID=Rating.mID group by Rating.mID
union select title,0 from Movie,Rating
where Movie.mID not in (select mID from Rating);

/* Q8 - For each movie that has at least one rating, return the title and the 
        'rating spread', that is, the difference between highest and lowest 
        ratings given to that movie. Sort by rating spread from highest to 
        lowest, then by movie title alphabetically.   
*/
select " ";
select "Q8";

/* Put your SQL for Q8 below */
select distinct title,H.stars-L.stars
from Movie,Rating H,Rating L
where Movie.mID=H.mID AND H.mID=L.mID
AND not exists (select * from Rating L where H.mID=L.mID AND L.stars>H.stars)
AND not exists (select * from Rating H where H.mID=L.mID AND H.stars<L.stars)
order by L.stars-H.stars,title;

/* Q9 -  Find the difference between the average rating of movies released before 
         1980 and the average rating of movies released after 1980. (Make sure to 
         calculate the average rating for each movie, then the average of those 
         averages for movies before 1980 and movies after. Don't just calculate 
         the overall average rating before and after 1980.)  
*/
select " ";
select "Q9";

/* Put your SQL for Q9 below */
select abs(avg(A)-avg(B))
from (select avg(stars) as A from Movie,Rating
where Movie.mID=Rating.mID AND Movie.year>1980 group by Rating.mID),
(select avg(stars) as B from Movie,Rating
where Movie.mID=RAting.mID AND Movie.year<1980 group by Rating.mID);

/* Q10 - For each director, return the director's name together with the title(s) 
         of the movie(s) they directed that received the highest rating among all 
         of their movies, and the value of that rating. 
*/
select " ";
select "Q10";

/* Put your SQL for Q10 below */
select director,title,max(stars)
from Movie,Rating where Movie.mID=Rating.mID 
group by Movie.director;