package de.otto.anthology

final case class QualifiedAggregateId(domainName: DomainName, aggregateName: AggregateName, id: AggregateId):
    lazy val qualifier: (DomainName, AggregateName) = (domainName, aggregateName)
    lazy val qualifierString: String = s"$domainName/$aggregateName"
    override def toString(): String = s"$qualifierString/$id"
