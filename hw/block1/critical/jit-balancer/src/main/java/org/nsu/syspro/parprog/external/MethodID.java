package org.nsu.syspro.parprog.external;

/**
 * Marker interface represents some method. Could be {@link ExecutionEngine#interpret(MethodID) interpreted} directly or
 * {@link CompilationEngine compiled} and then {@link ExecutionEngine#execute(CompiledMethod) executed}.
 * <br>
 * Unique {@link #id} is provided for convenience:
 * <ul>
 *     <li> Hashing
 *     <li> Faster identity checks: different {@link MethodID}s have different ids.
 * </ul>
 */
public abstract class MethodID {
    public abstract long id();

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MethodID)) {
            return false;
        }
        var m = (MethodID)o;
        return id() == m.id();
    }
}
