# WeblogSessionizationSpark
Sessionizing Weblogs using Spark/Scala

Please go to src/main/scala/com/paytm/challenge to see my scala source codes. 

I've conducted this challenge based of the following assumptions:

              (1) Sessions ONLY end when the user inactivity exceeds the inactivity threshold. The
                  inactivity threshold has been set to 900 secs (15 mins) - but can be changed in
                  settings object.
                  
              (2) I have supposed that sessions are not ended by users and each session lasts at least as
                  long as the inactivity threshold. For instance, If a client has only one activity in the
                  log (their IP appears only once in the entire log), then the session is assumed to be 15
                  minutes (= inactivity threshold)
                  
              (3) In summary, a session ends 15 mins after the user's last activity in the log, i.e. no sessions lives
                  less than 15 mins! ( I'm fair to sessions :) )
                  
  
  
Check sampleResults.txt to see some results. This is the output of 'Sessionization.scala'. 
