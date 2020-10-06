CREATE AGGREGATE time_aggregate(
    "time" TIMESTAMPTZ,
    value ANYELEMENT)
(
    sfunc=time_aggregate_trans,
    stype=internal,
    finalfunc=time_aggregate_final
);

CREATE OPERATOR |> (
    FUNCTION = aggregate_pipeline,
    LEFTARG = TimebucketAggregate,
    RIGHTARG = AggregatePipelineElement
);

CREATE OPERATOR @ (
    FUNCTION = unnest,
    LEFTARG = TimebucketAggregate,
    RIGHTARG = ANYELEMENT
);

CREATE OPERATOR |> (
    FUNCTION = aggregate_do_pipeline,
    LEFTARG = TimebucketAggregate,
    RIGHTARG = AggregatePipeline
);

CREATE OR REPLACE FUNCTION "start_aggregate_pipeline"("first" AggregatePipelineElement, "second" AggregatePipelineElement) RETURNS AggregatePipeline STRICT SET search_path TO @extschema@ LANGUAGE c IMMUTABLE AS 'MODULE_PATHNAME', 'start_aggregate_pipeline_wrapper';

CREATE OR REPLACE FUNCTION "add_to_aggregate_pipeline"("pipeline" AggregatePipeline, "operation" AggregatePipelineElement) RETURNS AggregatePipeline STRICT SET search_path TO @extschema@ LANGUAGE c IMMUTABLE AS 'MODULE_PATHNAME', 'add_to_aggregate_pipeline_wrapper';

CREATE OPERATOR |> (
    FUNCTION = start_aggregate_pipeline,
    LEFTARG = AggregatePipelineElement,
    RIGHTARG = AggregatePipelineElement
);

CREATE OPERATOR |> (
    FUNCTION = add_to_aggregate_pipeline,
    LEFTARG = AggregatePipeline,
    RIGHTARG = AggregatePipelineElement
);
