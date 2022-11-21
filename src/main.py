from processor_logic import ProcessorLogic
import globallogger

logger = globallogger.setup_custom_logger('app')
    
if __name__ == '__main__':
    # Start the sequential batch-to-stream processor
    logger.info("Starting Batch-To-Stream Processor.")
    ProcessorLogic().execute()
