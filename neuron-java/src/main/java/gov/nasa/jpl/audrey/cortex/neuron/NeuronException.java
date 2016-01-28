package gov.nasa.jpl.audrey.cortex.neuron;

public class NeuronException extends Exception {
    private static final long serialVersionUID = 1L;

    public NeuronException() { super(); }
    public NeuronException(String message) { super(message); }
    public NeuronException(String message, Throwable cause) { super(message, cause); }
    public NeuronException(Throwable cause) { super(cause); }
}
