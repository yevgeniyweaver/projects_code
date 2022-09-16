<?php

namespace App\Http\Controllers\Dsp;

use App\Http\Controllers\Controller;
use App\Models\ListDSP;
use App\Models\Project;
use App\Models\SettingsDSP;
use Illuminate\Http\Request;
use Illuminate\Pagination\LengthAwarePaginator;

class DspList extends Controller {

    public function getEndpointsByDspCompany(Request $request, $active, $integration, $region, $manager, $orderBy = "company_name", $sortBy = 'desc', $filters = null, $winrate = []) {
        $settingsDspTbl = new SettingsDSP();
        $settingsDspTblName = $settingsDspTbl->getTable();
        $listDsp = new ListDSP;
        $listDspTblName = $listDsp->getTable();
        $listDspCPName = "company_name";
        $needCollection =  false;
        $orderByCollection = false;
        $columnsOrderedByCollection = ["winrate"];
        $perPage = 50;
        $page = $request->has('page') ? (int)$request->page : 1;
        $filters = array_diff($filters, array('', false));

        $where = [];
        if ($active === false || $active === 2) {
            $where[] = ["$settingsDspTblName.active", '<>', -1];
            $whereForEp[] = ["active", '<>', -1];
        } else {
            $where[] = ["$settingsDspTblName.active", '=', $active];
            $whereForEp[] = ["active", '=', $active];
        }

        if ($active === 1) {
            $where[] = ["$settingsDspTblName.qps", '>', 0];
            $whereForEp[] = ["qps", '>', 0];
        } elseif ($active === 2) {
            $where[] = ["$settingsDspTblName.qps", '=', 0];
            $whereForEp[] = ["qps", '=', 0];
        }

        if ($integration !== false && $integration === $settingsDspTbl::INTEGRATION_RTB) {
            $where[] = ["$settingsDspTblName.usePrebid", '=', 0];
            $where[] = ["$settingsDspTblName.usexml", '=', 0];
            $where[] = ["$settingsDspTblName.useVast", '=', 0];
        } elseif ($integration !== false && $integration === $settingsDspTbl::INTEGRATION_PREBID) {
            $where[] = ["$settingsDspTblName.usePrebid", '=', 1];
        } elseif ($integration !== false && $integration === $settingsDspTbl::INTEGRATION_XML) {
            $where[] = ["$settingsDspTblName.usexml", '=', 1];
        } elseif ($integration !== false && $integration === $settingsDspTbl::INTEGRATION_VAST) {
            $where[] = ["$settingsDspTblName.useVast", '=', 1];
        }

        if ($region) {
            $where[] = ["$settingsDspTblName.region", '=', $region];
        }

        $whereMain = $where;
        // Only for main "where" condition, not for endpoints
        if ($manager) {
            $whereMain[] = ["$listDspTblName.account_manager_id", '=', $manager];
        }

        if(in_array($orderBy, $columnsOrderedByCollection)) {
            $needCollection = true;
            $orderByCollection = $orderBy;
            $orderBy = $listDspCPName;
        }
        $fullOrderBy = ($orderBy == 'company_name') ? "$listDspTblName.$orderBy" : "$orderBy";

        $select = $this->getSelectColumnsForEp();
        $listQuery = $listDsp
            ->selectRaw("$listDspTblName.company_name as `company_name`")
            ->selectRaw("$listDspTblName.account_manager_id as manager")
            ->selectRaw("MAX(`$settingsDspTblName`.`usebanner`) as `usebanner`")
            ->selectRaw("MAX(`$settingsDspTblName`.usevideo) as `usevideo`")
            ->selectRaw("MAX(`$settingsDspTblName`.usenative) as `usenative`")
            ->selectRaw("MAX(`$settingsDspTblName`.useaudio) as `useaudio`")
            ->selectRaw("MAX(`$settingsDspTblName`.usepop) as `usepop`")
            ->selectRaw("AVG(`$settingsDspTblName`.adaptraffic) as `adaptraffic`")
            ->selectRaw("GROUP_CONCAT(DISTINCT(`$settingsDspTblName`.region)) as `region`")
            ->selectRaw("MAX(`$settingsDspTblName`.allowVastRtb) as `allowVastRtb`")
            ->selectRaw("SUM(`$settingsDspTblName`.qps) as `qps`")
            ->selectRaw("ROUND(SUM(`$settingsDspTblName`.bid_qps), 2) as `bid_qps`")
            ->selectRaw("ROUND(SUM(`$settingsDspTblName`.real_qps), 2) as `real_qps`")
            ->selectRaw("ROUND(SUM(`$settingsDspTblName`.yesterdayspend), 2) as `yesterdayspend`")
            ->selectRaw("ROUND(SUM(`$settingsDspTblName`.dailyspend), 2) as `dailyspend`")
            ->selectRaw("ROUND(SUM(`$settingsDspTblName`.spendlimit), 2) as `spendlimit`")
            ->leftJoin($settingsDspTblName, "$listDspTblName.$listDspCPName", '=', "$settingsDspTblName.$listDspCPName");


        if (!empty($filters)) {
            $listQuery->where(function ($query) use ($filters, $settingsDspTblName) {
                foreach ($filters as $column => $value) {
                    $query->orWhere("$settingsDspTblName.$column", 'LIKE', "%{$value}%");
                }
            });
        }

        $filtersForEndpoints = $filters;
        unset($filtersForEndpoints['company_name']);

        $listQuery
            ->with(['endpoints' => function ($query) use ($select, $where, $filtersForEndpoints, $settingsDspTblName) {
                $query
                    ->select($select)
                    ->selectRaw("md5(concat(keyname, 1234)) as apikey")
                    ->where($where)
                    ->where(function ($query) use ($filtersForEndpoints, $settingsDspTblName) {
                        foreach ($filtersForEndpoints as $column => $value) {
                            $query->orWhere("$settingsDspTblName.$column", 'LIKE', "%{$value}%");
                        }
                    });
            }])
            ->where($whereMain);


        $listDspByCompany = $listQuery->groupBy("$listDspTblName.company_name")->orderBy($fullOrderBy, $sortBy);

        if ($needCollection) {
            $listDspCompany = $listDspByCompany->get()->toArray();
            $afterAgregate = $this->prepareListDspByCompany($listDspCompany, $winrate);
            $sortDesc = $sortBy === 'desc';
            $firstCollection = (collect($afterAgregate))->sortBy($orderByCollection, SORT_REGULAR, $sortDesc);
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
            $listDspCompany = $listDspByCompany->paginate(50)->toArray();
            /* add winRate column and change data array */
            $this->prepareListDspByCompany($listDspCompany['data'], $winrate);
            $result = $listDspCompany;
        }

        return $result;
    }

    private function getSelectColumnsForEp(): array {

        $select = [
            "id",
            "keyname",
            "endpoint",
            "usebanner",
            "usenative",
            "useaudio",
            "usevideo",
            "qps",
            "real_qps",
            "bid_qps",
            "region",
            "comments",
            "company_name",
            "active",
            "dailyspend",
            "spendlimit",
            "usepop",
            "adaptraffic",
            "allowedCountries",
            "desktop",
            "mobweb",
            "inapp",
            "intstlonly",
            "native_spec",
            "tmax",
            "maxbidfloor",
            "reqdevid",
            "reqdevid",
            "reqcarrier",
            "reqpubid",
            "requserid",
            "gzipResponses",
            "goodtraffic",
            "filterporn",
            "secureprotocol",
            "trafquality",
            "noMismatchedIpTraff",
            "noMismatchedBundles",
            "devos",
            "conntype",
            "blocked_ssp",
            "allowedSSP",
            "yesterdayspend",
            "allowVastRtb",
        ];

        if (in_array(app()->project->name, [Project::SMARTY_ADS])) {
            $select[] = "fraudPercPX";
            $select[] = "fraudPercPM";
            $select[] = "pxIpBlack";
            $select[] = "pxIfaBlack";
        } elseif (in_array(app()->project->name, [Project::GOTHAM_ADS])) {
            $select[] = "fraudPercPX";
            $select[] = "fraudPercPM";
            $select[] = "pxIpBlack";
            $select[] = "pxIfaBlack";
        } elseif (in_array(app()->project->name, [Project::BIZZ_CLICK])) {
            $select[] = "fraudPercPX";
            $select[] = "fraudPercPM";
            $select[] = "pxIpBlack";
            $select[] = "pxIfaBlack";
        } elseif (in_array(app()->project->name, [Project::ACUITY])) {
            $select[] = "marga";
            $select[] = "actmarga";
        } elseif (in_array(app()->project->name, [Project::ADVENUE])) {
            $select[] = "marga";
            $select[] = "fraudPercPM";
        } elseif (in_array(app()->project->name, [Project::ACEEX, Project::ACEEX2])) {
            $select[] = "marga";
            $select[] = "maxqps";
            $select[] = "pixalate";
            $select[] = "fraudPercPM";
        }

        if (in_array(app()->project->name, [Project::SMARTY_ADS, Project::ACUITY, Project::ADVENUE, Project::ACEEX])) {
            $select[] = "schain";
        }

        return $select;
    }


    private function prepareListDspByCompany(&$listDspByCompany, $winRate) {

        foreach ($listDspByCompany as $company => &$row) {
            $WinRate = [];
            foreach ($row['endpoints'] as $ep) {
                $WinRate[] = $winRate[$ep['keyname']]['winRate'] ?? 0;
            }
            if (in_array(app()->project->name, [Project::ACUITY])) {
                $row['companyActive'] = max(array_column($row['endpoints'], 'active'));
                $sumActiveEndpoints = 0;
                $counter = 0;
                foreach ($row['endpoints'] as $k => $endpoint) {
                    if ($endpoint['active'] && $endpoint['qps']) {
                        $sumActiveEndpoints += $endpoint['actmarga'];
                        $counter++;
                    }
                }
                $row['avgActMarga'] = $counter ? round($sumActiveEndpoints/$counter) : 0;
            } else {
                $row['avgActMarga'] = count($WinRate) != 0 ?
                    round(array_sum(array_column($row['endpoints'], 'actmarga'))/count($WinRate)) : 0;
            }
            $WinRateNotEmpty = array_filter($WinRate, function($val) {
                return ($val > 0);
            });
            $row['winrate'] = count($WinRateNotEmpty) != 0 ? array_sum($WinRateNotEmpty)/count($WinRateNotEmpty) : 0;//avgWinRate that is more than 0
        }

        return $listDspByCompany;
    }
}
