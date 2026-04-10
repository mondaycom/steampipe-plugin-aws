package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi"
	taggingTypes "github.com/aws/aws-sdk-go-v2/service/resourcegroupstaggingapi/types"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

//// TABLE DEFINITION

func tableAwsElastiCacheServerlessCache(_ context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "aws_elasticache_serverless_cache",
		Description: "AWS ElastiCache Serverless Cache",
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("serverless_cache_name"),
			Hydrate:    getElastiCacheServerlessCache,
			Tags:       map[string]string{"service": "elasticache", "action": "DescribeServerlessCaches"},
		},
		List: &plugin.ListConfig{
			Hydrate: listElastiCacheServerlessCaches,
			Tags:    map[string]string{"service": "elasticache", "action": "DescribeServerlessCaches"},
		},

		GetMatrixItemFunc: SupportedRegionMatrix(AWS_ELASTICACHE_SERVICE_ID),
		Columns: awsRegionalColumns([]*plugin.Column{
			{
				Name:        "serverless_cache_name",
				Description: "The unique identifier of the serverless cache.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "arn",
				Description: "The ARN (Amazon Resource Name) of the serverless cache.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("ARN"),
			},
			{
				Name:        "status",
				Description: "The current status of the serverless cache.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "engine",
				Description: "The name of the cache engine (e.g., redis, valkey) used by the serverless cache.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "full_engine_version",
				Description: "The version of the cache engine that is used in this serverless cache.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "create_time",
				Description: "The date and time when the serverless cache was created.",
				Type:        proto.ColumnType_TIMESTAMP,
			},
			{
				Name:        "subnet_ids",
				Description: "The subnet IDs for the serverless cache.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "security_group_ids",
				Description: "The security group IDs for the serverless cache.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "user_group_id",
				Description: "The ID of the user group associated with the serverless cache.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "cache_usage_limits",
				Description: "The cache usage limits for the serverless cache.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "daily_snapshot_time",
				Description: "The daily time range during which ElastiCache takes a snapshot of the serverless cache.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "snapshot_retention_limit",
				Description: "The number of days for which ElastiCache retains automatic serverless cache snapshots before deleting them.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "description",
				Description: "The description of the serverless cache.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "kms_key_id",
				Description: "The ID of the KMS key used to encrypt the serverless cache.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "endpoint",
				Description: "The endpoint information for the serverless cache.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "reader_endpoint",
				Description: "The reader endpoint information for the serverless cache.",
				Type:        proto.ColumnType_JSON,
			},
			{
				Name:        "tags_src",
				Description: "A list of tags associated with the serverless cache.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("TagList"),
			},

			// Standard columns
			{
				Name:        "title",
				Description: resourceInterfaceDescription("title"),
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("ServerlessCacheName"),
			},
			{
				Name:        "tags",
				Description: resourceInterfaceDescription("tags"),
				Type:        proto.ColumnType_JSON,
				Transform:   transform.From(serverlessCacheTagListToTurbotTags),
			},
			{
				Name:        "akas",
				Description: resourceInterfaceDescription("akas"),
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("ARN").Transform(transform.EnsureStringArray),
			},
		}),
	}
}

//// TYPES

type serverlessCacheWithTags struct {
	types.ServerlessCache
	TagList []types.Tag
}

//// LIST FUNCTION

func listElastiCacheServerlessCaches(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	// Create Session
	svc, err := ElastiCacheClient(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("aws_elasticache_serverless_cache.listElastiCacheServerlessCaches", "connection_error", err)
		return nil, err
	}

	// Batch fetch all tags for serverless caches in this region using the
	// Resource Groups Tagging API. This replaces N individual ListTagsForResource
	// calls with a single paginated GetResources call.
	tagMap := fetchServerlessCacheTagsBatch(ctx, d)

	input := &elasticache.DescribeServerlessCachesInput{
		MaxResults: aws.Int32(100),
	}

	if d.QueryContext.Limit != nil {
		limit := int32(*d.QueryContext.Limit)
		if limit < *input.MaxResults {
			if limit < 20 {
				input.MaxResults = aws.Int32(20)
			} else {
				input.MaxResults = aws.Int32(limit)
			}
		}
	}

	// List call
	paginator := elasticache.NewDescribeServerlessCachesPaginator(svc, input, func(o *elasticache.DescribeServerlessCachesPaginatorOptions) {
		o.Limit = *input.MaxResults
		o.StopOnDuplicateToken = true
	})

	for paginator.HasMorePages() {
		// apply rate limiting
		d.WaitForListRateLimit(ctx)

		output, err := paginator.NextPage(ctx)
		if err != nil {
			plugin.Logger(ctx).Error("aws_elasticache_serverless_cache.listElastiCacheServerlessCaches", "api_error", err)
			return nil, err
		}

		for _, serverlessCache := range output.ServerlessCaches {
			var tags []types.Tag
			if serverlessCache.ARN != nil {
				tags = tagMap[*serverlessCache.ARN]
			}

			d.StreamListItem(ctx, serverlessCacheWithTags{
				ServerlessCache: serverlessCache,
				TagList:         tags,
			})

			// Context can be cancelled due to manual cancellation or the limit has been hit
			if d.RowsRemaining(ctx) == 0 {
				return nil, nil
			}
		}
	}

	return nil, nil
}

//// HYDRATE FUNCTIONS

func getElastiCacheServerlessCache(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	// Create service
	svc, err := ElastiCacheClient(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("aws_elasticache_serverless_cache.getElastiCacheServerlessCache", "connection_error", err)
		return nil, err
	}

	serverlessCacheName := d.EqualsQuals["serverless_cache_name"].GetStringValue()

	// Return nil, if no input provided
	if serverlessCacheName == "" {
		return nil, nil
	}

	params := &elasticache.DescribeServerlessCachesInput{
		ServerlessCacheName: aws.String(serverlessCacheName),
	}

	op, err := svc.DescribeServerlessCaches(ctx, params)
	if err != nil {
		plugin.Logger(ctx).Error("aws_elasticache_serverless_cache.getElastiCacheServerlessCache", "api_error", err)
		return nil, err
	}

	if len(op.ServerlessCaches) == 0 {
		return nil, nil
	}

	cache := op.ServerlessCaches[0]

	// Fetch tags for this single resource
	var tags []types.Tag
	if cache.ARN != nil {
		tags = fetchServerlessCacheTagsSingle(ctx, d, *cache.ARN)
	}

	return serverlessCacheWithTags{
		ServerlessCache: cache,
		TagList:         tags,
	}, nil
}

//// TAG FUNCTIONS

// fetchServerlessCacheTagsBatch fetches tags for all elasticache serverless
// caches in the current region using the Resource Groups Tagging API.
// Returns a map of ARN -> []types.Tag. On error, returns an empty map
// so the list function can still return results without tags.
func fetchServerlessCacheTagsBatch(ctx context.Context, d *plugin.QueryData) map[string][]types.Tag {
	tagMap := make(map[string][]types.Tag)

	svc, err := ResourceGroupsTaggingClient(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Warn("aws_elasticache_serverless_cache.fetchServerlessCacheTagsBatch", "tagging_client_error", err)
		return tagMap
	}

	input := &resourcegroupstaggingapi.GetResourcesInput{
		ResourceTypeFilters: []string{"elasticache:serverlesscache"},
		ResourcesPerPage:    aws.Int32(100),
	}

	paginator := resourcegroupstaggingapi.NewGetResourcesPaginator(svc, input, func(o *resourcegroupstaggingapi.GetResourcesPaginatorOptions) {
		o.Limit = 100
		o.StopOnDuplicateToken = true
	})

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			plugin.Logger(ctx).Warn("aws_elasticache_serverless_cache.fetchServerlessCacheTagsBatch", "tagging_api_error", err)
			return tagMap
		}

		for _, resource := range output.ResourceTagMappingList {
			if resource.ResourceARN != nil {
				tagMap[*resource.ResourceARN] = convertTaggingToElastiCacheTags(resource.Tags)
			}
		}
	}

	return tagMap
}

// fetchServerlessCacheTagsSingle fetches tags for a single serverless cache
// using the ElastiCache ListTagsForResource API. Used by the Get function
// where batch fetching is unnecessary.
func fetchServerlessCacheTagsSingle(ctx context.Context, d *plugin.QueryData, arn string) []types.Tag {
	svc, err := ElastiCacheClient(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Warn("aws_elasticache_serverless_cache.fetchServerlessCacheTagsSingle", "connection_error", err)
		return nil
	}

	output, err := svc.ListTagsForResource(ctx, &elasticache.ListTagsForResourceInput{
		ResourceName: aws.String(arn),
	})
	if err != nil {
		plugin.Logger(ctx).Warn("aws_elasticache_serverless_cache.fetchServerlessCacheTagsSingle", "api_error", err)
		return nil
	}

	return output.TagList
}

// convertTaggingToElastiCacheTags converts Resource Groups Tagging API tags
// to ElastiCache tag types.
func convertTaggingToElastiCacheTags(taggingTags []taggingTypes.Tag) []types.Tag {
	tags := make([]types.Tag, len(taggingTags))
	for i, t := range taggingTags {
		tags[i] = types.Tag{Key: t.Key, Value: t.Value}
	}
	return tags
}

//// TRANSFORM FUNCTIONS

func serverlessCacheTagListToTurbotTags(ctx context.Context, d *transform.TransformData) (interface{}, error) {
	item := d.HydrateItem.(serverlessCacheWithTags)

	var turbotTagsMap map[string]string
	if len(item.TagList) > 0 {
		turbotTagsMap = map[string]string{}
		for _, i := range item.TagList {
			turbotTagsMap[*i.Key] = *i.Value
		}
	}

	return turbotTagsMap, nil
}
