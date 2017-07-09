Run the filtering tasks on the raw dataset.
Run FieldTagging.java on the papers left out after automated field tagging.
Run PaperDOIKeywordsEntry.java. Input : taggedFilteredMetadata.java , Output : PaperBagofPhrases.
Run UniqueDOICitationContext.java. Input : filteredCitations.txt , Output : DOIMergedContexts.
Run UniqueKeyphrasesCount.java. Input : PaperBagofPhrases, Output: GlobalKeyphrasesList.
Run ListKeywordsFromContext.java. Input : DOIMergedContexts & GlobalKeyphrasesList  , Output: DOITerms.
Run UnionKeyphrasesContextEntry.java. Input : DOITerms & PaperBagofPhrases , Output : PaperBagofPhrases.
Run CitationNetworkDirect.java. Input: filteredCitations.txt , Output : CitationNetworksFinal.
Run CitationNetworkIndirect.java. Input: CitationNetworksFinal & PaperBagofPhrases , Output : CitationNetworksFinal.
Run PaperDOICommunityEntry.java. Input : taggedfilteredMetadata.txt , Output : PaperNagofPhrases.
Run CreateCommunityGraph.java. Input : CitationNetworksFinal & PaperBagofPhrases , Output : CommunityNetworkDirect.
Run CreateContextCommunityGraph.java. Input : CitationNetworksFinal & PaperBagofPhrases , Output : ContextBasedCommunityNetwork.
Run CommunityCount.java. Input : PaperBagofPhrases ,Output : CommunityAnalysisFinal
Run MetricsExpansionDirect.java and MetricsExpansionIndirect.java.
Run MetricsCutRatioDirect.java and MetricsCutRatioIndirect.java.
Run MetricConductance.java and MetricConductanceIndirect.java.
Run PaperMetricIndegreeOutdegreeDirect/Indirect.java and PaperMetricIndegreeOutdegreePatch.java(change the context.write entries in the reducer task) for both the networks after each task.
Run MetricMedian.java. (change the column family of in the map task)
Run MetricFOMDDirect/Indirect.java.
Run MetricsInwardness.java (update column family to run for direct and indirect networks).
