package pdsp.sentimentAnalysis;

public interface SentimentClassifier {
    public void initialize();
    public SentimentResult classify(String str);
}