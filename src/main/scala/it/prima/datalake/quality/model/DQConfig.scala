package it.prima.datalake.quality.model

case class DQConfig(analyze: Option[Boolean] = None,
                    groups: Option[List[DQGroup]] = None
                   )

case class DQGroup(name: String,
                   description: String,
                   severity: String,
                   checks: List[DQConstraint]
                  )

case class DQConstraint(columns: List[String],
                        check_type: String,
                        operator: Option[String] = None,
                        value: Option[String] = None,
                        value_list: Option[List[String]] = None
                       )
