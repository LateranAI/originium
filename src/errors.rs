use thiserror::Error;

#[derive(Error, Debug)]
pub enum FrameworkError {
    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Component build error: Failed to build {component_type} for endpoint configuration '{endpoint_description}'. Reason: {reason}")]
    ComponentBuildError {
        component_type: String, // e.g., "Reader" or "Writer"
        endpoint_description: String,  // e.g., a file path, URL, or relevant part of DataEndpoint
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
        item_description: String, // A brief description of the item being transformed
        reason: String,
    },

    #[error("Internal framework error: {0}")]
    InternalError(String),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error("Channel send error while sending to {channel_description}: {error_message}")]
    ChannelSendError{
        channel_description: String, // e.g., "main input broker" or "writer specific channel"
        error_message: String,
    },
    
    #[error("No suitable component (Reader/Writer) found for endpoint: {0}")]
    NoComponentFound(String),
}

// Helper for converting Box<dyn Error> into FrameworkError for pipeline errors
// impl FrameworkError {
//     pub fn pipeline_error<E: std::error::Error + Send + Sync + 'static>(component_name: &str, error: E) -> Self {
//         FrameworkError::PipelineError {
//             component_name: component_name.to_string(),
//             source: Box::new(error),
//         }
//     }
// } 