package params

/**
 * Created by diego on 11/24/14.
 */

import org.apache.spark.sql._

object Files {

  private val formatDateWithHour = ((field: String) => {
    val formatter = new java.text.SimpleDateFormat("MMM dd, yyyy, 'Hour' kk")
    formatter.setTimeZone(java.util.TimeZone.getTimeZone(Misc.TimeZone));
    new java.sql.Timestamp(formatter.parse(field).getTime())
  })

  private val formatDate = ((field: String) => {
    if(field == "")
      null
    else {
      val format = if (field.contains("/")) "MM/DD/YYYY hh:mm:ss a" else "yyyy-MM-dd HH:mm:ss.SSS"
      val formatter = new java.text.SimpleDateFormat(format)
      formatter.setTimeZone(java.util.TimeZone.getTimeZone(Misc.TimeZone))
      new java.sql.Timestamp(formatter.parse(field).getTime())
    }
  })

  // input files and formatting
  private val BasePath = "/Users/diego/Box Sync/Data/Onvia/project data/"

  val File = Map(
    "saved_search" -> Map(
      "file" -> (BasePath + "SavedSearchResults/SavedSearchProjects.csv"),
      "delimiter" -> "\",\"",
      "table" -> "saved_search",
      "formatting" -> Map()
    ),
    "company_activity" -> Map(
      "file" -> (BasePath + "CompanyActivity/CompanyActivity.csv"),
      "delimiter" -> "\",\"",
      "table" -> "company_activity",
      "formatting" -> Map()
    ),
    "sent" -> Map(
      "file" -> (BasePath + "UserActivity/ProjectsSent.csv"),
      "delimiter" -> "\",\"",
      "table" -> "sent",
      "formatting" -> Map(
        "SentDate" -> Map(
          "index" -> 2,
          "type" -> TimestampType,
          "function" -> formatDateWithHour
        )
      )
    ),
    "trashed" -> Map(
      "file" -> (BasePath + "UserActivity/ProjectActions.csv"),
      "delimiter" -> "\",\"",
      "table" -> "trashed",
      "formatting" -> Map(
        "ActionDate" -> Map(
          "index" -> 5,
          "type" -> TimestampType,
          "function" -> formatDateWithHour
        )
      )
    ),
    "project_locations"-> Map(
      "file" -> (BasePath + "ProjectData/ProjectLocation.csv"),
      "delimiter" -> "\",\"",
      "table" -> "project_locations",
      "formatting" -> Map()
    ),
    "geo_locations"-> Map(
      "file" -> (BasePath + "ProjectData/GeoMapping.csv"),
      "delimiter" -> ",",
      "table" -> "geo_locations",
      "formatting" -> Map()
    ),
    "project_categories"-> Map(
      "file" -> (BasePath + "ProjectData/ProjectCategories.csv"),
      "delimiter" -> "\",\"",
      "table" -> "project_categories",
      "formatting" -> Map()
    ),
    "project_attributes" -> Map(
      "file" -> (BasePath + "ProjectData/ProjectSVAttributes.csv"),
      "delimiter" -> "\",\"",
      "table" -> "project_attributes",
      "formatting" -> Map(
        "PublicationDate" -> Map(
          "index" -> 21,
          "type" -> TimestampType,
          "function" -> formatDate
        ),
        "SubmittalDateTime" -> Map(
          "index" -> 10,
          "type" -> TimestampType,
          "function" -> formatDate
        )
      )
    ),
    "clicks" -> Map(
      "file" -> (BasePath + "UserActivity/ProjectClicks.csv"),
      "delimiter" -> "\",\"",
      "table" -> "clicks",
      "formatting" -> Map(
        "ClickDate" -> Map(
          "index" -> 2,
          "type" -> TimestampType,
          "function" -> formatDateWithHour
        )
      )
    ),
    "project_tags" -> Map(
      "file" -> (BasePath + "ProjectData/ProjectTags.csv"),
      "delimiter" -> "\",\"",
      "table" -> "project_tags",
      "formatting" -> Map(
        "Weight" -> Map(
          "index" -> 3,
          "type" -> IntegerType,
          "function" -> ((field: String) => {
            if(field == "NULL") null else field.toInt
          })
        )
      )
    ),
    "agency_details" -> Map(
      "file" -> (BasePath + "AgencyDetails/AgencyDetails.csv"),
      "delimiter" -> "\",\"",
      "table" -> "agency_details",
      "formatting" -> Map(
        "EmployeeCountTotal" -> Map(
          "index" -> 1,
          "type" -> IntegerType,
          "function" -> ((field: String) => {
            if(field == "NULL") null else field.toInt
          })
        ),
        "Population" -> Map(
          "index" -> 3,
          "type" -> IntegerType,
          "function" -> ((field: String) => {
            if(field == "NULL") null else field.toInt
          })
        ),
        "TotalExpenditure" -> Map(
          "index" -> 4,
          "type" -> IntegerType,
          "function" -> ((field: String) => {
            if(field == "NULL") null else field.toInt
          })
        ),
        "TotalRevenue" -> Map(
          "index" -> 5,
          "type" -> IntegerType,
          "function" -> ((field: String) => {
            if(field == "NULL") null else field.toInt
          })
        )
      )
    )
  )

}
