package gov.nasa.jpl.audrey.cortex.neuron;

public class NeuronOperationException extends NeuronException {
    private static final long serialVersionUID = 1L;

    public NeuronOperationException() { super(); }
    public NeuronOperationException(String message) { super(message); }
    public NeuronOperationException(String message, Throwable cause) { super(message, cause); }
    public NeuronOperationException(Throwable cause) { super(cause); }
}
