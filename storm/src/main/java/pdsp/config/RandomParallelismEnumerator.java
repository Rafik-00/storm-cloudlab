package pdsp.config;

import java.util.Random;

public class RandomParallelismEnumerator {
    private final int availableCores;
    private final Random random;

    public RandomParallelismEnumerator() {
        this.availableCores = Runtime.getRuntime().availableProcessors();
        this.random = new Random();
        System.out.println("Available Cores: "+ this.availableCores);
    }

    public int getRandomParallelismHint() {
        return random.nextInt(availableCores) + 1;
    }
}
