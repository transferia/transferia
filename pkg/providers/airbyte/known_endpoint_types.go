//go:build !disable_airbyte_provider

package airbyte

type EndpointType int

const (
	S3                  = EndpointType(24)
	Redshift            = EndpointType(25)
	MSSQL               = EndpointType(26)
	Bigquery            = EndpointType(27)
	Salesforce          = EndpointType(28)
	AmazonAds           = EndpointType(29)
	AmazonSellerPartner = EndpointType(30)
	AwsCloudtrail       = EndpointType(31)
	GoogleAds           = EndpointType(32)
	GoogleAnalytics     = EndpointType(33)
	GoogleSheets        = EndpointType(34)
	BingAds             = EndpointType(35)
	LinkedinAds         = EndpointType(36)
	FacebookMarketing   = EndpointType(37)
	FacebookPages       = EndpointType(38)
	TiktokMarketing     = EndpointType(39)
	SnapchatMarketing   = EndpointType(40)
	Instagram           = EndpointType(41)
	Snowflake           = EndpointType(47)
	Jira                = EndpointType(50)
	Hubspot             = EndpointType(51)
)

var DefaultImages = map[EndpointType]string{
	AmazonAds:           "airbyte/source-amazon-ads:0.1.3",
	AmazonSellerPartner: "airbyte/source-amazon-seller-partner:0.2.14",
	AwsCloudtrail:       "airbyte/source-aws-cloudtrail:0.1.4",
	Bigquery:            "airbyte/source-bigquery:0.1.8",
	BingAds:             "airbyte/source-bing-ads:0.1.3",
	FacebookMarketing:   "airbyte/source-facebook-marketing:0.2.35",
	FacebookPages:       "airbyte/source-facebook-pages:0.1.6",
	GoogleAds:           "airbyte/source-google-ads:0.1.27",
	GoogleAnalytics:     "airbyte/source-google-analytics-v4:0.1.16",
	GoogleSheets:        "airbyte/source-google-sheets:0.2.9",
	Instagram:           "airbyte/source-instagram:0.1.9",
	LinkedinAds:         "airbyte/source-linkedin-ads:2.1.2",
	MSSQL:               "airbyte/source-mssql:0.3.17",
	Redshift:            "airbyte/source-redshift:0.3.8",
	S3:                  "airbyte/source-s3:0.1.18",
	Salesforce:          "airbyte/source-salesforce:0.1.23",
	SnapchatMarketing:   "airbyte/source-snapchat-marketing:0.1.4",
	TiktokMarketing:     "airbyte/source-tiktok-marketing:0.1.3",
	Snowflake:           "airbyte/source-snowflake:0.1.32",
	Jira:                "airbyte/source-jira:0.10.2",
	Hubspot:             "airbyte/source-hubspot:0.2.1",
}
