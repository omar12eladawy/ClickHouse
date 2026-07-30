#include <Processors/Transforms/AsyncInsertQueueTransform.h>

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Sources/WaitForAsyncInsertSource.h>

namespace DB
{

namespace
{

/// Size the block would take once queued, without expanding it. A `ColumnConst` or a sparse column
/// stores one value for the whole block, so `byteSize` undercounts it, while materializing to find
/// out would allocate the expansion even for a block that is about to be rejected.
size_t estimateMaterializedBytes(const Columns & columns, size_t rows)
{
    size_t total = 0;
    for (const auto & column : columns)
    {
        if (isColumnConst(*column) || column->isSparse())
            total += column->byteSizeAt(0) * rows;
        else
            total += column->byteSize();
    }
    return total;
}

}

AsyncInsertQueueTransform::AsyncInsertQueueTransform(
    SharedHeader header_,
    AsynchronousInsertQueue * queue_,
    ContextMutablePtr context_,
    ASTPtr query_ast_,
    Names insert_column_names_,
    UInt64 max_data_size_,
    UInt64 wait_timeout_ms_)
    : ExceptionKeepingTransform(header_, header_, /* ignore_on_start_and_finish */ false)
    , queue(queue_)
    , context(std::move(context_))
    , query_ast(std::move(query_ast_))
    , insert_column_names(std::move(insert_column_names_))
    , max_data_size(max_data_size_)
    , wait_timeout_ms(wait_timeout_ms_)
{
}

void AsyncInsertQueueTransform::onConsume(Chunk chunk)
{
    if (chunk.getNumRows() == 0)
        return;

    if (!queued_eligible)
    {
        pending.push_back(std::move(chunk));
        return;
    }

    if (!held)
    {
        if (estimateMaterializedBytes(chunk.getColumns(), chunk.getNumRows()) > max_data_size)
        {
            queued_eligible = false;
            pending.push_back(std::move(chunk));
            return;
        }

        auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
        materializeBlockInplace(block);
        Chunk materialized(block.getColumns(), block.rows());

        /// The estimate is per value, so a column of varying-width values can still overshoot.
        if (block.bytes() > max_data_size)
        {
            queued_eligible = false;
            pending.push_back(std::move(materialized));
        }
        else
        {
            held = std::move(materialized);
        }
        return;
    }

    /// A second non-empty block means the result is not a single block.
    queued_eligible = false;
    pending.push_back(std::move(*held));
    held.reset();
    pending.push_back(std::move(chunk));
}

bool AsyncInsertQueueTransform::canGenerate()
{
    return !pending.empty();
}

AsyncInsertQueueTransform::GenerateResult AsyncInsertQueueTransform::onGenerate()
{
    GenerateResult res;
    res.chunk = std::move(pending.front());
    pending.pop_front();
    res.is_done = pending.empty();
    return res;
}

AsyncInsertQueueTransform::GenerateResult AsyncInsertQueueTransform::getRemaining()
{
    if (held)
    {
        auto block = getInputPort().getHeader().cloneWithColumns(held->detachColumns());
        held.reset();

        auto async_query = query_ast->clone();
        auto & async_insert_query = async_query->as<ASTInsertQuery &>();
        /// The pushed block is Preprocessed (Native-encoded). `preprocessInsertQuery` rejects an
        /// empty format, and a plain `INSERT ... SELECT` carries none, so set `Native` explicitly.
        async_insert_query.format = "Native";
        async_insert_query.columns = make_intrusive<ASTExpressionList>();
        for (const auto & name : insert_column_names)
            async_insert_query.columns->children.push_back(make_intrusive<ASTIdentifier>(name));
        auto result = queue->pushQueryWithBlock(async_query, std::move(block), context);
        /// `report_read_progress=false`: reads were already counted by `CountingTransform`.
        waitForAsyncInsertAndReportProgress(
            result.future, wait_timeout_ms,
            context->getProcessListElement(), context->getProgressCallback(),
            /* report_read_progress */ false);
    }

    return {};
}

}
