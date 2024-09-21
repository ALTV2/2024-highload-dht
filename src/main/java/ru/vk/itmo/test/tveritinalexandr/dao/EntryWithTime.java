package ru.vk.itmo.test.tveritinalexandr.dao;

import ru.vk.itmo.dao.Entry;

public record EntryWithTime<D>(D key, D value, long timeStamp) implements Entry<D> {
    @Override
    public String toString() {
        return "{" + key + ":" + value + "," + timeStamp + "}";
    }
}
