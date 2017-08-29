define(function (require) {
  const _ = require('lodash');
  const angular = require('angular');
  const moment = require('moment');
  const ConfigTemplate = require('ui/ConfigTemplate');
  const getSort = require('ui/doc_table/lib/get_sort');
  const rison = require('ui/utils/rison');

  const dateMath = require('ui/utils/dateMath');
  const stateMonitorFactory = require('ui/state_management/state_monitor_factory');

  require('ui/doc_table');
  require('ui/visualize');
  require('ui/notify');
  require('ui/fixedScroll');
  require('ui/directives/validate_json');
  require('ui/filters/moment');
  require('ui/courier');
  require('ui/index_patterns');
  require('ui/state_management/app_state');
  require('ui/timefilter');
  require('ui/highlight/highlight_tags');
  require('ui/share');

  const app = require('ui/modules').get('apps/discover', [
    'kibana/notify',
    'kibana/courier',
    'kibana/index_patterns'
  ]);

  require('ui/routes')
  .when('/discover/:id?', {
    template: require('plugins/kibana/discover/index.html'),
    reloadOnSearch: false,
    resolve: {
      ip: function (Promise, courier, config, $location) {
        return courier.indexPatterns.getIds()
        .then(function (list) {
          const stateRison = $location.search()._a;

          let state;
          try { state = rison.decode(stateRison); }
          catch (e) { state = {}; }

          const specified = !!state.index;
          const exists = _.contains(list, state.index);
          const id = exists ? state.index : config.get('defaultIndex');

          return Promise.props({
            list: list,
            loaded: courier.indexPatterns.get(id),
            stateVal: state.index,
            stateValFound: specified && exists
          });
        });
      },
      savedSearch: function (courier, savedSearches, $route) {
        return savedSearches.get($route.current.params.id)
        .catch(courier.redirectWhenMissing({
          'search': '/discover',
          'index-pattern': '/settings/objects/savedSearches/' + $route.current.params.id
        }));
      }
    }
  });

  app
  .directive('discoverApp', function () {
    return {
      controllerAs: 'discoverApp',
      controller: discoverController,
    };
  });

  function discoverController($scope, config, courier, $route, $window, Notifier,
    AppState, timefilter, Promise, Private, kbnUrl, highlightTags,es,kbnIndex) {

    const Vis = Private(require('ui/Vis'));
    const docTitle = Private(require('ui/doc_title'));
    const brushEvent = Private(require('ui/utils/brush_event'));
    const HitSortFn = Private(require('plugins/kibana/discover/_hit_sort_fn'));
    const queryFilter = Private(require('ui/filter_bar/query_filter'));
    const filterManager = Private(require('ui/filter_manager'));

    const notify = new Notifier({
      location: 'Discover'
    });

	//const定义的变量不可以修改，而且必须初始化	
	//let是块级作用域，函数内部使用let定义后，对函数外部无影响
	//var定义的变量可以修改，如果不初始化会输出undefined，不会报错
	//search body 

	/*$('#myquery').focus(function(){
		$('#myquery').val('test');
	});

	$('#myquery').blur(function(){
		$('#myquery').val('hello');
	});*/

	/*$scope.change_query = function(x){
		console.log("************************************");
		console.log(x);//Object { query_string: Object }
		console.log(x.query_string);
		console.log(x.query_string.query);
		if(x.query_string.query.indexOf('_type:')>0){
			//x.query_string.query = null
			$('#myquery').val('test');
		}
		if(x.query_string.query.indexOf('host:')>0){
			//x.query_string.query = null
			$('#myquery').val('hello');
		}
		console.log("************************************");
	}*/

	////当改变type时，自动刷新对应host列表
	$scope.change_type = function(x){
		console.log('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX');
		console.log($scope.indexPattern.id);
		let body = {
			"query": {
				"filtered": {
					"query": {
					"match_all": {}
					},
					"filter": {
						"term": {
							"_id": $scope.indexPattern.id
						}
					}
				}
			}
		};
		console.log(x);//$scope.query1 
		console.log("query = " + $scope.state.query.query_string.query);
		console.log('state.query1 = '+$state.query1);
		console.log('state.query2 = '+$state.query2);
		//当切换类型时，重置query ='*' query2=null
		if($('#myquery').val()!='*'){
			$('#myquery').val('*');
		}	
		//$scope.state.query.query_string.query = '*';
		console.log("query = " + $scope.state.query.query_string.query);
		console.log('YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY');

		$scope.hostgroups = []
		if(x != null){
			es.search({
				index: kbnIndex,
				type: 'select-buffter',
				body: body
			}).then(function(resp){
				if(resp.hits.total == 0) return;
				/*let len = resp.aggregations.group_by_type.buckets.length;
				for(let n=0; n<len; n++){//for 
					let key = resp.aggregations.group_by_type.buckets[n].key;
					let selected_type = x;
					console.log("key = "+key+","+"selected_type = "+selected_type);
					if(key == selected_type){
						let length = resp.aggregations.group_by_type.buckets[n].group_by_host.buckets.length;
						console.log("length = "+length.toString());
						for(let i=0; i<length; i++){
							$scope.hostgroups.push(resp.aggregations.group_by_type.buckets[n].group_by_host.buckets[i].key);
						}
					}
				}//end for*/
				// resp.hits.hits[0]._source.host
				console.log('resp.hits.hits.length = ',resp.hits.hits.length.toString());
				console.log('timestamp = ',resp.hits.hits[0]._source.timestamp)
				let len = resp.hits.hits[0]._source.buckets.length;
				for(let n=0; n<len; n++){//for 
					let key = resp.hits.hits[0]._source.buckets[n].type;
					let selected_type = x;
					console.log("key = "+key+","+"selected_type = "+selected_type);
					if(key == selected_type){
						let length = resp.hits.hits[0]._source.buckets[n].hosts.length;
						console.log("length = "+length.toString());
						for(let i=0; i<length; i++){
							$scope.hostgroups.push(resp.hits.hits[0]._source.buckets[n].hosts[i]);
						}
					}
				}//end for 

				console.log("$scope.hostgroups = ",$scope.hostgroups.toString());	
			});
		}else{
			console.log("--请选择--");
		}	
			
		console.log('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX');
	}
	
	//
	$scope.change_host = function(x){
		console.log('hhhhhhhhhhhhhhhhhhhhhhh');
		console.log(x);
		console.log($state.query2);
		console.log('hhhhhhhhhhhhhhhhhhhhhhh');
	}

	////初始化
	$scope.getDyActivityInforItems = function(){
		console.log('///////////////////////////////////');
		console.log(es);
		console.log(kbnIndex);//.kibana
		console.log($scope.indexPattern.id);//waf-log 
		console.log("11111111111111111111");
		let body = {
			"query": {
				"filtered": {
					"query": {
					"match_all": {}
					},
					"filter": {
						"term": {
							"_id": $scope.indexPattern.id
						}
					}
				}
			}
		};

		es.search({
			index: kbnIndex,
			type: 'select-buffter',
			body: body
		}).then(function(resp){
			//console.log('resp.hits.hist.length = '+resp.hits.hits.length.toString());
			if(resp.hits.total == 0) return;
			$scope.typegroups = [];
			let len = resp.hits.hits[0]._source.buckets.length;
			console.log('len ======'+len.toString());
			for(let n=0; n<len; n++){
				$scope.typegroups.push(resp.hits.hits[0]._source.buckets[n].type);
				console.log('key === '+resp.hits.hits[0]._source.buckets[n].type);
			}
			console.log("typegroups = "+ $scope.typegroups.toString());
			
			//selected type is NULL
			console.log("66666666666666666666666666666");
			//初始化时，根据当前索引设置type列表选项
			//如果type初始值非空，则根据type当前值，刷新对应type的host列表
			$scope.hostgroups = [];
			//select host from $scope.index group by type;
			if($state.query1 != null){
				for(n=0; n<len; n++){
					let key = resp.hits.hits[0]._source.buckets[n].type;
					let selected_type = $state.query1;
					if (key==selected_type){
						let length = resp.hits.hits[0]._source.buckets[n].hosts.length;;
						console.log("length = "+length.toString());
						for(let i=0; i<length; i++){
							$scope.hostgroups.push(resp.hits.hits[0]._source.buckets[n].hosts[i]);
						}
					}
				}
				console.log('hostgroups = '+$scope.hostgroups.toString());
			}else{
				console.log("$state.query1 is null");
			}
			console.log($state.query2);
			console.log($scope.state.query2);
			console.log("777777777777777777777777777777");
			//selected values 
			// 3684188
			//console.log(resp.hits.total);
			// 20
			//console.log(resp.hits.hits.length);
			// 192.168.141.65
			//console.log(resp.hits.hits[0]._type);
			//---> type groups 

			//const poslist = $scope.poslist = [{select_code:"1",select_name: resp.hits.hits[0]._type}];
			//console.log(resp.hits.hits[0]._source.host);
			console.log("44444444444444444444444444");
		});

		//////
		/*let zindex = [];
		es.cat.indices({h:'index',index: $scope.indexPattern.id,format: 'json'}).then(function(resp){	
			console.log(resp);
			for(var n=0; n<resp.length; n++){
				zindex.push(resp[n].index);
			}
			console.log("222222222222222222222");
			//Array [ "syslog-waf-2017.08.04", "syslog-waf-2017.08.03", "syslog-waf-2017.08.02", "syslog-waf-2017.08.08", "syslog-waf-2017.08.07", "syslog-waf-2017.08.06", "syslog-waf-2017.08.05" ]
			console.log(zindex);

			console.log("@@@@@@@@@@@@@@@@@@@@@@@@");
		});*/

		console.log('///////////////////////////////////');
	}
	///////

    $scope.intervalOptions = Private(require('ui/agg_types/buckets/_interval_options'));
    $scope.showInterval = false;

    $scope.intervalEnabled = function (interval) {
      return interval.val !== 'custom';
    };

    $scope.toggleInterval = function () {
      $scope.showInterval = !$scope.showInterval;
    };

    // config panel templates
    $scope.configTemplate = new ConfigTemplate({
      load: require('plugins/kibana/discover/partials/load_search.html'),
      save: require('plugins/kibana/discover/partials/save_search.html'),
      share: require('plugins/kibana/discover/partials/share_search.html')
    });

    $scope.timefilter = timefilter;

    // the saved savedSearch
    const savedSearch = $route.current.locals.savedSearch;
    $scope.$on('$destroy', savedSearch.destroy);

    // the actual courier.SearchSource
    $scope.searchSource = savedSearch.searchSource;
    $scope.indexPattern = resolveIndexPatternLoading();
    $scope.searchSource.set('index', $scope.indexPattern);

    if (savedSearch.id) {
      docTitle.change(savedSearch.title);
    }

    let stateMonitor;
    const $appStatus = $scope.appStatus = this.appStatus = {};
    const $state = $scope.state = new AppState(getStateDefaults());
    $scope.uiState = $state.makeStateful('uiState');

    function getStateDefaults() {
      return {
        query: $scope.searchSource.get('query') || '',
        sort: getSort.array(savedSearch.sort, $scope.indexPattern),
        columns: savedSearch.columns || ['_source'],
        index: $scope.indexPattern.id,
        interval: 'auto',
        filters: _.cloneDeep($scope.searchSource.getOwn('filter'))
      };
    }

    $state.index = $scope.indexPattern.id;
    $state.sort = getSort.array($state.sort, $scope.indexPattern);

    $scope.$watchCollection('state.columns', function () {
      $state.save();
    });

    $scope.opts = {
      // number of records to fetch, then paginate through
      sampleSize: config.get('discover:sampleSize'),
      // Index to match
      index: $scope.indexPattern.id,
      timefield: $scope.indexPattern.timeFieldName,
      savedSearch: savedSearch,
      indexPatternList: $route.current.locals.ip.list
    };

    const init = _.once(function () {
      const showTotal = 5;
      $scope.failuresShown = showTotal;
      $scope.showAllFailures = function () {
        $scope.failuresShown = $scope.failures.length;
      };
      $scope.showLessFailures = function () {
        $scope.failuresShown = showTotal;
      };

      stateMonitor = stateMonitorFactory.create($state, getStateDefaults());
      stateMonitor.onChange((status) => {
        $appStatus.dirty = status.dirty;
      });
      $scope.$on('$destroy', () => stateMonitor.destroy());

      $scope.updateDataSource()
      .then(function () {
        $scope.$listen(timefilter, 'fetch', function () {
          $scope.fetch();
        });

        $scope.$watchCollection('state.sort', function (sort) {
          if (!sort) return;

          // get the current sort from {key: val} to ["key", "val"];
          const currentSort = _.pairs($scope.searchSource.get('sort')).pop();

          // if the searchSource doesn't know, tell it so
          if (!angular.equals(sort, currentSort)) $scope.fetch();
        });

        // update data source when filters update
        $scope.$listen(queryFilter, 'update', function () {
          return $scope.updateDataSource().then(function () {
            $state.save();
          });
        });

        // update data source when hitting forward/back and the query changes
        $scope.$listen($state, 'fetch_with_changes', function (diff) {
          if (diff.indexOf('query') >= 0) $scope.fetch();
        });

        // fetch data when filters fire fetch event
        $scope.$listen(queryFilter, 'fetch', $scope.fetch);

        $scope.$watch('opts.timefield', function (timefield) {
          timefilter.enabled = !!timefield;
        });

        $scope.$watch('state.interval', function (interval, oldInterval) {
          if (interval !== oldInterval && interval === 'auto') {
            $scope.showInterval = false;
          }
          $scope.fetch();
        });

        $scope.$watch('vis.aggs', function () {
          // no timefield, no vis, nothing to update
          if (!$scope.opts.timefield) return;

          const buckets = $scope.vis.aggs.bySchemaGroup.buckets;

          if (buckets && buckets.length === 1) {
            $scope.intervalName = 'by ' + buckets[0].buckets.getInterval().description;
          } else {
            $scope.intervalName = 'auto';
          }
        });

        $scope.$watchMulti([
          'rows',
          'fetchStatus'
        ], (function updateResultState() {
          let prev = {};
          const status = {
            LOADING: 'loading', // initial data load
            READY: 'ready', // results came back
            NO_RESULTS: 'none' // no results came back
          };

          function pick(rows, oldRows, fetchStatus) {
            // initial state, pretend we are loading
            if (rows == null && oldRows == null) return status.LOADING;

            const rowsEmpty = _.isEmpty(rows);
            // An undefined fetchStatus means the requests are still being
            // prepared to be sent. When all requests are completed,
            // fetchStatus is set to null, so it's important that we
            // specifically check for undefined to determine a loading status.
            const preparingForFetch = _.isUndefined(fetchStatus);
            if (preparingForFetch) return status.LOADING;
            else if (rowsEmpty && fetchStatus) return status.LOADING;
            else if (!rowsEmpty) return status.READY;
            else return status.NO_RESULTS;
          }

          return function () {
            const current = {
              rows: $scope.rows,
              fetchStatus: $scope.fetchStatus
            };

            $scope.resultState = pick(
              current.rows,
              prev.rows,
              current.fetchStatus,
              prev.fetchStatus
            );

            prev = current;
          };
        }()));

        $scope.searchSource.onError(function (err) {
          notify.error(err);
        }).catch(notify.fatal);

        function initForTime() {
          return setupVisualization().then($scope.updateTime);
        }

        return Promise.resolve($scope.opts.timefield && initForTime())
        .then(function () {
          init.complete = true;
          $state.replace();
          $scope.$emit('application.load');
        });
      });
    });

    $scope.opts.saveDataSource = function () {
      return $scope.updateDataSource()
      .then(function () {
        savedSearch.id = savedSearch.title;
        savedSearch.columns = $scope.state.columns;
        savedSearch.sort = $scope.state.sort;

        return savedSearch.save()
        .then(function (id) {
          stateMonitor.setInitialState($state.toJSON());
          $scope.configTemplate.close('save');

          if (id) {
            notify.info('Saved Data Source "' + savedSearch.title + '"');
            if (savedSearch.id !== $route.current.params.id) {
              kbnUrl.change('/discover/{{id}}', { id: savedSearch.id });
            } else {
              // Update defaults so that "reload saved query" functions correctly
              $state.setDefaults(getStateDefaults());
            }
          }
        });
      })
      .catch(notify.error);
    };

	//////////////////////////////summit 
	$scope.infetch = function(){
		console.log("55555555555555555555");
		console.log($('#myquery').val());
		//直接提取输入内容
		if(!(($('#myquery').val()).indexOf('_type:')<0)){
			$('#myquery').val('*');
		}
		if(!(($('#myquery').val()).indexOf('host:')<0)){
			$('#myquery').val('*');
		}
		let str = $('#myquery').val();
		
		//console.log($scope);
		console.log($('#myquery').val());
		console.log('##########################');
		//_type:waf AND host:192.168.141.65
		console.log($scope.state.query.query_string.query);
		//waf 
		console.log($scope.state.query1);
		//192.168.141.65 
		console.log($scope.state.query2);
		console.log('str = '+str);
		console.log('##########################');

		//reset 
		let q_string = $scope.state.query.query_string.query = str;
		let q1 = $scope.state.query1;
		let q2 = $scope.state.query2;
			
		console.log("@@@@@@@@@@@@@@@@@@@@@@@@@");
		//*
		console.log(q_string);
		//waf 
		console.log(q1);
		//192.168.141 
		console.log(q2);
		console.log('@@@@@@@@@@@@@@@@@@@@@@@@@@');

		//update $scope.state.query.query_string.query
		if(q1==null && q2==null){
			$scope.state.query.query_string.query = str;
		}else{
			if(q1!=null && q2==null){
				if(str!='*'){
					$scope.state.query.query_string.query = q_string + ' ' + 'AND _type:' + q1;				
				}else{
					$scope.state.query.query_string.query = '_type:' + q1;
				}
			}
			if(q1==null && q2!=null){
				if(str!='*'){
					$scope.state.query.query_string.query = q_string + ' ' + 'AND host:' + q2;
				}else{
					$scope.state.query.query_string.query = 'host:' + q2;
				}
			}
			if(q1!=null && q2!=null){
				if(str!='*'){
					$scope.state.query.query_string.query = q_string +' '+'AND _type:'+q1+' '+'AND host:'+q2;
				}else{
					$scope.state.query.query_string.query = '_type:'+q1+' '+'AND host:'+q2;
				}
			}
		}
		//_type waf AND host:192.168.141.65 
		console.log($scope.state.query.query_string.query);
		console.log("666666666666666666666");
	}

	////////
	//
    $scope.opts.fetch = $scope.fetch = function () {
		$scope.infetch();	
      // ignore requests to fetch before the app inits
      if (!init.complete) return;
	
	  //$scope.getDyActivityInforItems();
      $scope.updateTime();

      $scope.updateDataSource()
      .then(setupVisualization)
      .then(function () {
        $state.save();
        return courier.fetch();
      })
      .catch(notify.error);

	  //
	  //$scope.state.query.query_string.query = $('#myquery').val();
    };

    $scope.searchSource.onBeginSegmentedFetch(function (segmented) {

      function flushResponseData() {
        $scope.hits = 0;
        $scope.faliures = [];
        $scope.rows = [];
        $scope.fieldCounts = {};
      }

      if (!$scope.rows) flushResponseData();

      const sort = $state.sort;
      const timeField = $scope.indexPattern.timeFieldName;
      const totalSize = $scope.size || $scope.opts.sampleSize;

      /**
       * Basically an emum.
       *
       * opts:
       *   "time" - sorted by the timefield
       *   "non-time" - explicitly sorted by a non-time field, NOT THE SAME AS `sortBy !== "time"`
       *   "implicit" - no sorting set, NOT THE SAME AS "non-time"
       *
       * @type {String}
       */
      const sortBy = (function () {
        if (!_.isArray(sort)) return 'implicit';
        else if (sort[0] === '_score') return 'implicit';
        else if (sort[0] === timeField) return 'time';
        else return 'non-time';
      }());

      let sortFn = null;
      if (sortBy !== 'implicit') {
        sortFn = new HitSortFn(sort[1]);
      }

      $scope.updateTime();
      if (sort[0] === '_score') segmented.setMaxSegments(1);
      segmented.setDirection(sortBy === 'time' ? (sort[1] || 'desc') : 'desc');
      segmented.setSortFn(sortFn);
      segmented.setSize($scope.opts.sampleSize);

      // triggered when the status updated
      segmented.on('status', function (status) {
        $scope.fetchStatus = status;
      });

      segmented.on('first', function () {
        flushResponseData();
      });

      segmented.on('segment', notify.timed('handle each segment', function (resp) {
        if (resp._shards.failed > 0) {
          $scope.failures = _.union($scope.failures, resp._shards.failures);
          $scope.failures = _.uniq($scope.failures, false, function (failure) {
            return failure.index + failure.shard + failure.reason;
          });
        }
      }));

      segmented.on('mergedSegment', function (merged) {
        $scope.mergedEsResp = merged;
        $scope.hits = merged.hits.total;

        const indexPattern = $scope.searchSource.get('index');

        // the merge rows, use a new array to help watchers
        $scope.rows = merged.hits.hits.slice();

        notify.event('flatten hit and count fields', function () {
          let counts = $scope.fieldCounts;

          // if we haven't counted yet, or need a fresh count because we are sorting, reset the counts
          if (!counts || sortFn) counts = $scope.fieldCounts = {};

          $scope.rows.forEach(function (hit) {
            // skip this work if we have already done it
            if (hit.$$_counted) return;

            // when we are sorting results, we need to redo the counts each time because the
            // "top 500" may change with each response, so don't mark this as counted
            if (!sortFn) hit.$$_counted = true;

            const fields = _.keys(indexPattern.flattenHit(hit));
            let n = fields.length;
            let field;
            while (field = fields[--n]) {
              if (counts[field]) counts[field] += 1;
              else counts[field] = 1;
            }
          });
        });
      });

      segmented.on('complete', function () {
        if ($scope.fetchStatus.hitCount === 0) {
          flushResponseData();
        }

        $scope.fetchStatus = null;
      });
    }).catch(notify.fatal);

    $scope.updateTime = function () {
      $scope.timeRange = {
        from: dateMath.parse(timefilter.time.from),
        to: dateMath.parse(timefilter.time.to, true)
      };
    };

    $scope.resetQuery = function () {
      kbnUrl.change('/discover/{{id}}', { id: $route.current.params.id });
    };

    $scope.newQuery = function () {
      kbnUrl.change('/discover');
    };

    $scope.updateDataSource = Promise.method(function () {
      $scope.searchSource
      .size($scope.opts.sampleSize)
      .sort(getSort($state.sort, $scope.indexPattern))
      .query(!$state.query ? null : $state.query)
      .set('filter', queryFilter.getFilters());

      if (config.get('doc_table:highlight')) {
        $scope.searchSource.highlight({
          pre_tags: [highlightTags.pre],
          post_tags: [highlightTags.post],
          fields: {'*': {}},
          require_field_match: false,
          fragment_size: 2147483647 // Limit of an integer.
        });
      }
    });

    // TODO: On array fields, negating does not negate the combination, rather all terms
    $scope.filterQuery = function (field, values, operation) {
      $scope.indexPattern.popularizeField(field, 1);
      filterManager.add(field, values, operation, $state.index);
    };

    $scope.toTop = function () {
      $window.scrollTo(0, 0);
    };

    let loadingVis;
    function setupVisualization() {
      // If we're not setting anything up we need to return an empty promise
      if (!$scope.opts.timefield) return Promise.resolve();
      if (loadingVis) return loadingVis;

      const visStateAggs = [
        {
          type: 'count',
          schema: 'metric'
        },
        {
          type: 'date_histogram',
          schema: 'segment',
          params: {
            field: $scope.opts.timefield,
            interval: $state.interval,
            min_doc_count: 0
          }
        }
      ];

      // we have a vis, just modify the aggs
      if ($scope.vis) {
        const visState = $scope.vis.getState();
        visState.aggs = visStateAggs;

        $scope.vis.setState(visState);
        return Promise.resolve($scope.vis);
      }

      $scope.vis = new Vis($scope.indexPattern, {
        title: savedSearch.title,
        type: 'histogram',
        params: {
          addLegend: false,
          addTimeMarker: true
        },
        listeners: {
          click: function (e) {
            console.log(e);
            timefilter.time.from = moment(e.point.x);
            timefilter.time.to = moment(e.point.x + e.data.ordered.interval);
            timefilter.time.mode = 'absolute';
          },
          brush: brushEvent
        },
        aggs: visStateAggs
      });

      $scope.searchSource.aggs(function () {
        $scope.vis.requesting();
        return $scope.vis.aggs.toDsl();
      });

      // stash this promise so that other calls to setupVisualization will have to wait
      loadingVis = new Promise(function (resolve) {
        $scope.$on('ready:vis', function () {
          resolve($scope.vis);
        });
      })
      .finally(function () {
        // clear the loading flag
        loadingVis = null;
      });

      return loadingVis;
    }

    function resolveIndexPatternLoading() {
      const props = $route.current.locals.ip;
      const loaded = props.loaded;
      const stateVal = props.stateVal;
      const stateValFound = props.stateValFound;

      const own = $scope.searchSource.getOwn('index');

      if (own && !stateVal) return own;
      if (stateVal && !stateValFound) {
        const err = '"' + stateVal + '" is not a configured pattern. ';
        if (own) {
          notify.warning(err + ' Using the saved index pattern: "' + own.id + '"');
          return own;
        }

        notify.warning(err + ' Using the default index pattern: "' + loaded.id + '"');
      }
      return loaded;
    }

    init();
  }
});
