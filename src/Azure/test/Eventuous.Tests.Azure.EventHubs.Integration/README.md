# Azure Event Hubs Integration Tests

This project contains integration tests for the Azure Event Hubs Eventuous implementation.

## Status: TODO

The complex integration tests from the original test project have been moved here for future development. These tests require:

- Actual Azure Event Hubs resources
- Azure Blob Storage resources
- Azure Table Storage resources
- Complex setup and teardown procedures
- Significant refactoring to align with current Eventuous API

## Current Focus

The unit tests in `Eventuous.Tests.Azure.EventHubs.Unit` provide comprehensive coverage of the core functionality and are fully working.

## Future Work

When ready to implement integration tests:

1. Set up Azure resources (Event Hubs, Blob Storage, Table Storage)
2. Refactor tests to match current Eventuous API
3. Implement proper test fixtures and setup/teardown
4. Add comprehensive integration test scenarios

## Test Categories

- **Unit Tests**: ✅ Working in `Eventuous.Tests.Azure.EventHubs.Unit`
- **Integration Tests**: 🚧 TODO in this project
- **End-to-End Tests**: 🚧 TODO in this project
