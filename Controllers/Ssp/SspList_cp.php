<?php
namespace App\Http\Controllers\Ssp;

use App\Http\Controllers\Controller;
use App\Models\ListSSP;
use App\Models\SettingsSSP;
use App\Models\SspTrafficQuality;
use App\Models\TrafficQualityTypes;
use Illuminate\Http\Request;
use Illuminate\Pagination\LengthAwarePaginator;

class SspList {

    public function getEndpointsBySspCompany(Request $request, $active, $integration, $winRate, $region, $manager, $orderBy = "company_name", $sortBy, $filters) {
        $settingsSspTbl = new SettingsSSP();
        $settingsSspTblName = $settingsSspTbl->getTable();
        $listSSP = new ListSSP();
        $listSspTblName = $listSSP->getTable();
        $sspTraffTypes = (new SspTrafficQuality)->getTable();
        $traffTypes = (new TrafficQualityTypes)->getTable();
        $listSspPKName = "company_name";
        $needCollection = false;
        $orderByCollection = false;
        $columnsOrderedByCollection = ["winrate", "blockbytmt"];
        $perPage = 50;
        $page = $request->has('page') ? (int)$request->page : 1;
        $filters = array_diff($filters, array('', false));

        $where = [];
        if ($active === false) {
            $where[] = ["$settingsSspTblName.active", '<>', $settingsSspTbl::STATUS_ARCHIVE];
        } else {
            $where[] = ["$settingsSspTblName.active", '=', $active];
        }

        if ($integration !== false && $integration === $settingsSspTbl::INTEGRATION_RTB) {
            $where[] = ["$settingsSspTblName.isPrebid", '=', 0];
            $where[] = ["$settingsSspTblName.isPushXML", '=', 0];
            $where[] = ["$settingsSspTblName.isVast", '=', 0];
            $where[] = ["$settingsSspTblName.isDirectPub", '=', 0];
        } elseif ($integration !== false && $integration === $settingsSspTbl::INTEGRATION_PREBID) {
            $where[] = ["$settingsSspTblName.isPrebid", '=', 1];
        } elseif ($integration !== false && $integration === $settingsSspTbl::INTEGRATION_XML) {
            $where[] = ["$settingsSspTblName.isPushXML", '=', 1];
        } elseif ($integration !== false && $integration === $settingsSspTbl::INTEGRATION_VAST) {
            $where[] = ["$settingsSspTblName.isVast", '=', 1];
        } elseif ($integration !== false && $integration === $settingsSspTbl::INTEGRATION_PUB) {
            $where[] = ["$settingsSspTblName.isDirectPub", '=', 1];
        }

        if(in_array($orderBy, $columnsOrderedByCollection)) {
            $needCollection = true;
            $orderByCollection = $orderBy;
            $orderBy = $listSspPKName;
        }
        $fullOrderBy = ($orderBy == 'company_name') ? "$listSspTblName.$orderBy" : "$orderBy";

        if ($region) {
            $where[] = ["$settingsSspTblName.region", '=', $region];
        }

        $whereMain = $where;
        // Only for main "where" condition, not for endpoints
        if ($manager) {
            $whereMain[] = ["$listSspTblName.account_manager_id", '=', $manager];
        }

        $select = [
            "$settingsSspTblName.*",
        ];

        $listQuery = $listSSP
            ->selectRaw("$listSspTblName.company_name as `company_name`")
            ->selectRaw("$listSspTblName.account_manager_id as manager")
            ->selectRaw("GROUP_CONCAT(DISTINCT`ExchangeSSPSetings`.region ORDER BY `ExchangeSSPSetings`.region ASC) as `region`")
            ->selectRaw("AVG(`$settingsSspTblName`.active) as `active`")
            ->selectRaw("ROUND(SUM(`$settingsSspTblName`.qps), 2) as `qps`")
            ->selectRaw("ROUND(SUM(`$settingsSspTblName`.bidqps), 2) as `bidqps`")
            ->selectRaw("ROUND(SUM(`$settingsSspTblName`.yesterdayspend), 2) as `yesterdayspend`")
            ->selectRaw("ROUND(SUM(`$settingsSspTblName`.dailyspend), 2) as `dailyspend`")
            ->selectRaw("ROUND(SUM(`$settingsSspTblName`.spendlimit), 2) as `spendlimit`")
            ->leftJoin($settingsSspTblName, "$settingsSspTblName.$listSspPKName", '=', "$listSspTblName.$listSspPKName");

        if (!empty($filters)) {
            $listQuery->where(function ($query) use ($filters, $settingsSspTblName) {
                foreach ($filters as $column => $value) {
                    $query->orWhere("$settingsSspTblName.$column", 'LIKE', "%{$value}%");
                }
            });
        }

        $filtersForEndpoints = $filters;
        unset($filtersForEndpoints['company_name']);

        $listQuery
            ->with(['endpoints' => function ($query) use ($select, $where, $sspTraffTypes, $traffTypes, $settingsSspTblName, $filtersForEndpoints) {
                $query->select($select)
                    ->selectRaw("GROUP_CONCAT($sspTraffTypes.type_id) as traffType")
                    ->selectRaw("GROUP_CONCAT($traffTypes.type) as traffTypeNames")
                    ->leftJoin($sspTraffTypes, "$settingsSspTblName.id", '=', "$sspTraffTypes.ssp_id")
                    ->leftJoin($traffTypes, "$traffTypes.id", '=', "$sspTraffTypes.type_id")
                    ->where($where)
                    ->where(function ($query) use ($filtersForEndpoints, $settingsSspTblName) {
                        foreach ($filtersForEndpoints as $column => $value) {
                            $query->orWhere("$settingsSspTblName.$column", 'LIKE', "%{$value}%");
                        }
                    })
                    ->groupBy("$settingsSspTblName.id")
                    ->orderBy("$settingsSspTblName.statname");
            }])
            ->where($whereMain);


        $listSspByCompany = $listQuery
            ->groupBy("$listSspTblName.company_name")
            ->orderBy($fullOrderBy, $sortBy);

        if ($needCollection) {
            $listSspCompany = $listSspByCompany->get()->toArray();
            $afterAgregate = $this->prepareListSspByCompany($listSspCompany, $winRate, $settingsSspTbl::TRANSFORM_TRAFF_TYPES);
            $sortDesc = $sortBy == 'desc';
            $firstCollection = (collect($afterAgregate))->sortBy($orderByCollection, SORT_NUMERIC, $sortDesc);
            $sortedCollection = $firstCollection->values();

            $paginator = new LengthAwarePaginator(
                ($sortedCollection->forPage($page, $perPage))->values(),
                $sortedCollection->count(),
                $perPage,
                $page,
                ['path' => $request->url(), 'query' => $request->query()]
            );
            $result = $paginator->toArray();
        } else {
            $beforeAgregate = $listSspByCompany->paginate(50)->toArray();
            $this->prepareListSspByCompany($beforeAgregate['data'], $winRate, $settingsSspTbl::TRANSFORM_TRAFF_TYPES);
            $result = $beforeAgregate;
        }

        return $result;
    }


    private function prepareListSspByCompany(&$listSspByCompany, $winRate, array $settingsSSPTraffTypes)
    {
        foreach ($listSspByCompany as $k => &$company) {
            $WinRate = [];
            $traffTypeFromEndpoints = [];
            $isPop = [];
            $blockbytmt = [];
            $hasNoSpendLimitEp = false;
            foreach($company['endpoints'] as $key => &$endpoint) {
                $WinRate[] = $winRate[$endpoint['statname']]['winRate'] ?? 0;

                if ($endpoint['ispop']) {
                    $isPop[] = $endpoint['ispop'];
                }

                if ($endpoint['blockbytmt']) {
                    $blockbytmt[] = $endpoint['blockbytmt'];
                }

                $arrayTypes = [];
                if (isset($endpoint['traffTypeNames'])) {
                    $arrayTypes = explode(",", $endpoint['traffTypeNames']);
                }

                $endpoint['traffType'] = ($endpoint['traffType'] != null) ? explode(",", $endpoint['traffType']) : [1];
                $transformTraffTypes = [];
                if (!empty($arrayTypes)) {
                    foreach ($arrayTypes as $r => $type) {
                        $transformTraffTypes[] = $settingsSSPTraffTypes[$type];
                    }
                } else {
                    $transformTraffTypes[] = 'unc';
                }
                $endpoint['transformTraffTypes'] = implode(',', $transformTraffTypes);

                if (!empty($endpoint['transformTraffTypes'])) {
                    $traffTypeFromEndpoints = array_merge($traffTypeFromEndpoints, explode(",", $endpoint['transformTraffTypes']));
                }

                if ($endpoint['active'] == 1) {
                    if ($endpoint['spendlimit'] == 0) {
                        $hasNoSpendLimitEp = true;
                    }
                    if ($hasNoSpendLimitEp) {
                        $company['spendlimit']  = 0;
                    } else {
                        $company['spendlimit']  += $endpoint['spendlimit'];
                    }
                }
            }
            $company['blockbytmt'] = count($blockbytmt) == count($company['endpoints']) ? 1 : 0;
            $company['ispop'] = count($isPop) == count($company['endpoints']) ? 1 : 0;
            $company['traffTypeNames'] = count($traffTypeFromEndpoints) ? implode(',', array_unique($traffTypeFromEndpoints)) : '';
            $WinRateNotEmpty = array_filter($WinRate, function($val) {
                return ($val > 0);
            });
            $company['winrate'] = count($WinRateNotEmpty) != 0 ? array_sum($WinRateNotEmpty)/count($WinRateNotEmpty) : 0;//avgWinRate that is more than 0

        }

        return $listSspByCompany;
    }

}
