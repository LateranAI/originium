use thiserror::Error;

#[derive(Error, Debug)]
pub enum FrameworkError {
    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Component build error: Failed to build {component_type} for endpoint configuration '{endpoint_description}'. Reason: {reason}")]
    ComponentBuildError {
        component_type: String,
        endpoint_description: String,
        reason: String,
    },

    #[error("Pipeline execution error in component {component_name}: {source}")]
    PipelineError {
        component_name: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[error("Unsupported endpoint type '{endpoint_description}' for the requested operation: {operation_description}")]
    UnsupportedEndpointType {
        endpoint_description: String,
        operation_description: String,
    },
    
    #[error("Data transformation error for item: {item_description}. Reason: {reason}")]
    TransformError{
        item_description: String,
        reason: String,
    },

    #[error("Internal framework error: {0}")]
    InternalError(String),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error("Channel send error while sending to {channel_description}: {error_message}")]
    ChannelSendError{
        channel_description: String,
        error_message: String,
    },
    
    #[error("No suitable component (Reader/Writer) found for endpoint: {0}")]
    NoComponentFound(String),
}









