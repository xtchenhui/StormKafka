
// set up data structures
window.streams = {};
streams.home = [];
streams.users = {};
streams.users.shawndrost = [];
streams.users.sharksforcheap = [];
streams.users.mracus = [];
streams.users.douglascalhoun = [];
window.users = Object.keys(streams.users);

var source = new EventSource('/stream');
var hash = {};
var width = 1200;
var height = 700;
var tweetContent = {};

source.onmessage = function (event) {
  word = event.data.split("|")[0];
  count = event.data.split("|")[1];
  
  //document.body.innerHTML += event.data + '<br>';
  tweetContent = event.data;
};

// utility function for adding tweets to our data structures
var addTweet = function(tweetContent){
  var username = "test";
  //streams.users[username].push(tweetContent);
  streams.home.push(tweetContent);
};

var parseTweet = function(){
  var tweet = {};
  //tweet.user = randomElement(users);
  tweet.message = tweetContent;
  tweet.created_at = new Date();
  addTweet(tweet);
};

for(var i = 0; i < 10; i++){
  parseTweet();
}

var scheduleNextTweet = function(){
  parseTweet();
  setTimeout(scheduleNextTweet, Math.random() * 150);
};
scheduleNextTweet();


//clean list, can be added to word skipping bolt
var skipList = ["https","follow","1","2","please","following","followers","fucking","RT","the","at","a"];

var skip = function(tWord){
  for(var i=0; i<skipList.length; i++){
    if(tWord === skipList[i]){
      return true;
    }
  }
  return false;
};

