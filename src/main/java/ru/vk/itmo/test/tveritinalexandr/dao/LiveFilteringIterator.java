package ru.vk.itmo.test.tveritinalexandr.dao;

import ru.vk.itmo.dao.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Filters non tombstone {@link EntryWithTime}s.
 *
 * @author incubos
 */
final class LiveFilteringIterator implements Iterator<EntryWithTime<MemorySegment>> {
    private final Iterator<EntryWithTime<MemorySegment>> delegate;
    private EntryWithTime<MemorySegment> next;

    LiveFilteringIterator(final Iterator<EntryWithTime<MemorySegment>> delegate) {
        this.delegate = delegate;
        skipTombstones();
    }

    private void skipTombstones() {
        while (delegate.hasNext()) {
            final EntryWithTime<MemorySegment> entry = delegate.next();
            if (entry.value() != null) {
                this.next = entry;
                break;
            }
        }
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public EntryWithTime<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        // Consume
        final EntryWithTime<MemorySegment> result = next;
        next = null;

        skipTombstones();

        return result;
    }
}
