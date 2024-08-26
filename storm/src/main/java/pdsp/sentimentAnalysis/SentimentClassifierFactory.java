package pdsp.sentimentAnalysis;

public class SentimentClassifierFactory {
    public static final String LINGPIPE = "lingpipe";
    public static final String BASIC    = "basic";
    
    public static SentimentClassifier create(String classifierName) {
        SentimentClassifier classifier;
        
        switch (classifierName) {
            case BASIC:
                classifier = new BasicClassifier();
                break;
            case LINGPIPE:
                classifier = (SentimentClassifier) new LingPipeClassifier();
                break;
            default:
                throw new IllegalArgumentException("There is not sentiment classifier named " + classifierName);
        }
        
        classifier.initialize();
        
        return classifier;
    }
}
