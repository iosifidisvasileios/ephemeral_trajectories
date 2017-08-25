#Tracking Ephemeral Sentiment Entities in SocialStreams

With the rise of Web 2.0,  more and more words that are not sentimental are often
associated with positive/negative feelings. In the following Apache Spark project we present an approach 
for extracting and monitoring what we call ephemeral entities from social streams.

The project generates: 
- **sentiment analysis** (using lexicon-based approach SentiWordNet 3.0 )
- **not lexicon words extraction** (generating a dictionary for each month)
- **assigning sentiment to not lexicon words** (using methods avg and pmi)
- **applying FEL entities filtering** 
- **create trajectories of ephemeral entities**

The dataset should follow the following format using as the tab character a separator:
- ID(long)
- POST_DATE ("EEE MMM dd HH:mm:ss Z yyyy")
- TWEET (string)
- SENTIMENT_OF_TEXT (positive/negative/neutral)

Example:
> 737783382323535872      Tue May 31 23:10:40 +0000 2016  CODE RED! MESQUITE TX - NEED ADOPTION INTEREST/RESCUETAG EMAILED BEFORE 7AM, 06/01! rescues@cityofmesquite.com https://t.co/vfvcBBmbe3       neutral

The project was built using Scala 2.10.6 and Spark version 1.6.0 for supporting the distributed computation. 

- **MainSentiment.scala** : Object that is in charge of calling to the different methods for applying the preprocessing, the sentiment analysis tasks and building the dictionaries containing the words that are not
part of the lexicon using both methods. 
It writes the following files as outputs :
    * *sentimentDataset.csv*: file that contains the dataset after applying the preprocessing and sentiment analysis tasks. The output format
uses the character separator ",".	
      * dataset sentiment
      * sentiwordnet sentiment
      * tweet after preprocessing
      * no sentiment words + the sentiwordnet sentiment (case tweets)
      * no sentiment words + the dataset sentiment (case tweets)
      * no sentiment words + the sentiwordnet sentiment (case tokens)
      * no sentiment words + the dataset sentiment (case tokens)
      * hashtags words
      * slangs words that come from the first call
      * slangs words that come from the second call
      * list of no frequent words (less than the set threshold) 
   * *Hashtags.csv* (contains the hashtags found during the preprocessing and sentiment analysis tasks)
   * *WordsNoSentimentImplComb.csv*: (groups the words that are not part of the lexicon along with the sentiment from sentiwordnet case tokens)
   * *WordsNoSentimentOrigComb.csv*: (groups the words that are not part of the lexicon along with the sentiment from dataset case tokens)
   * *WordsNoSentimentImplCombDist.csv*: (groups the words that are not part of the lexicon along with the sentiment from sentiwordnet case tweets)
   * *WordsNoSentimentOrigCombDist.csv*: (groups the words that are not part of the lexicon along with the sentiment from dataset case tweets)
   * *AVGSentWordNetTokens.csv*: (dictionary of not sentiment words method avg case tokens)
   * *AVGSentWordNetTweets.csv*: (dictionary of not sentiment words method avg case tweets)
   * *SentiWordNetPMITweets.txt*: (dictionary of not sentiment words method avg case tweets)
   * *SentiWordNetPMITokens.txt*: (dictionary of not sentiment words method avg case tokens)
- **PrParameters.scala:** object that contains the paths of the  models, datasets, libraries and other external files necessary for the project
- **CombFelEntities.scala:** object takes the list of the FEL entities and apply some preprocessing steps */
- **BuildTrajectories.scala:** object that gathers all dictionaries of not sentiment words, apply some filters i.e. FEl entities as output it writes into a file the ephemeral entities trajectory

 
