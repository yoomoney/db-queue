package ru.yoomoney.tech.dbqueue.stub;

import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionOperations;

/**
 * @author Oleg Kandaurov
 * @since 04.08.2017
 */
public class FakeTransactionTemplate implements TransactionOperations {
    @Override
    public <T> T execute(TransactionCallback<T> action) throws TransactionException {
        return action.doInTransaction(new SimpleTransactionStatus());
    }
}
