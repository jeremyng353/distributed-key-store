package com.g2.CPEN431.A7;

public class Pair<A, B> {
    A first = null;
    B second = null;

    /**
     * This is the constructor for the Pair initializing first and second
     * @param first: The first value in the pair of type A
     * @param second: The second value in the pair of type B
     */
    Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    /**
     * This function gets the first value in the pair
     * @return The first value in the pair of type A
     */
    public A getFirst() {
        return first;
    }

    /**
     * This function sets the first value in the pair
     */
    public void setFirst(A first) {
        this.first = first;
    }

    /**
     * This function gets the second value in the pair
     * @return The second value in the pair of type B
     */
    public B getSecond() {
        return second;
    }

    /**
     * This function sets the second value in the pair
     */
    public void setSecond(B second) {
        this.second = second;
    }
}
