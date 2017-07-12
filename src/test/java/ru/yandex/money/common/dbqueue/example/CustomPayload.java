package ru.yandex.money.common.dbqueue.example;

/**
 * @author Oleg Kandaurov
 * @since 12.07.2017
 */
class CustomPayload {
    private final String type;
    private final String description;

    CustomPayload(String type, String description) {
        this.type = type;
        this.description = description;
    }

    String getType() {
        return type;
    }

    String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return "CustomPayload{" +
                "type='" + type + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
